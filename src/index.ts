import dotenv from 'dotenv'
dotenv.config()

import { isPlainObject, mapKeys, mapValues, update } from 'lodash'
import MySQL from 'mysql2/promise'

const { identify: identifyQuery } = require('sql-query-identifier')

export type MySQLResult = MySQL.RowDataPacket[] | MySQL.RowDataPacket[][] | MySQL.OkPacket | MySQL.OkPacket[] | MySQL.ResultSetHeader

const pool: MySQL.Pool = MySQL.createPool({
	host: process.env['MYSQL_HOST'],
	user: process.env['MYSQL_USER'],
	password: process.env['MYSQL_PASSWORD'],
	database: process.env['MYSQL_DATABASE'],
	namedPlaceholders: true
})

/**
 * Process the values to be queried.
 * @param value The value you want to process.
 * @param options Additional options:
 * ```
 * const options: Options = {
 * - json: 'Whether you want to parse Object's and Array's to a JSON String.'
 * }
 * ```
 */
function processValue(value: any, options?: { json?: boolean; }): any {
	// typeof value === 'function'
	// typeof value === 'object'
	// typeof value === 'symbol'
	// typeof value === 'undefined'
	const bypassableTypes = [ 'bigint', 'boolean', 'number', 'string' ]
	if (value === undefined) return null
	if (bypassableTypes.includes(typeof value)) return value
	if (Array.isArray(value) || isPlainObject(value)) {
		if (options?.json === true) return JSON.stringify(value)
		return mapValues(value, processValue)
	}
	return String(value)
}
/**
 * query(string, values)
 * string: ':var' -> values: { var: value }
 * string: '?' -> values: value
 * string: '?, ?' -> values: [ value1, value2 ]
*/

/**
 * Parses your Query values by your SQL Query String.
 * @param queryString Query string of your request.
 * @param values Values you need to put in your query.
 * @param options Additional options.  
 * ---
 * - `options`.`force`: Uses bruteforce to prevent TypeError's by converting all invalid values to `null`. It's recommended to treat that errors by applying `NOT NULL` option on creating a new key.
 */
function parseQueryStringValues(queryString: string, values?: any, options?: { force?: boolean }): any {
	if ([null, undefined].includes(values)) return null
	
	const bruteforce = options?.force === true
	
	const namedPlaceholders = queryString.match(/:[a-z]+/ig) || []
	const [ { parameters: placeholders } ] = identifyQuery(queryString, { dialect: 'mysql' })
	
	const cause = {
		query_string: queryString,
		placeholders: placeholders,
		named_placeholders: namedPlaceholders,
		values
	}
	// console.log('Placeholders', { position: placeholders, named: namedPlaceholders })
	
	if (placeholders.length === 1) {
		if (Array.isArray(values)) return [ processValue(values[0], { json: true }) ]
		else if (![ 'bigint', 'boolean', 'number', 'string' ].includes(typeof values)) return processValue(values, { json: true })
		// console.log('Debug:', values, namedPlaceholders, placeholders)
		return [ values ]
	}
	if (placeholders.length > 0 && Array.isArray(values)) return values.map(v => processValue(v, { json: true }))
	if (namedPlaceholders.length > 0 && isPlainObject(values)) {
		const keys = Object.keys(values)
		return Object.fromEntries(namedPlaceholders.map((k: `:${string}`) => {
			const key = k.slice(1)
			if (!keys.includes(key)) {
				if (bruteforce) return [key, null]
				else throw new Error(`Parameter '${key}' not found on values object.`, { cause })
			}
			return [key, processValue(values[key], { json: true })]
		}))
	}

	return null
}

// Working!
async function query(string: string, values?: any, connection?: MySQL.Connection | MySQL.PoolConnection): Promise<[ MySQL.RowDataPacket[] | MySQL.RowDataPacket[][] | MySQL.OkPacket | MySQL.OkPacket[] | MySQL.ResultSetHeader, MySQL.FieldPacket[] ]> {
	const conn: MySQL.Connection | MySQL.PoolConnection = connection ? connection : await pool.getConnection()
	const parsedValues = parseQueryStringValues(string, values)
	console.log('Query', { string, values: parsedValues })
	const response = await conn.execute(string, parsedValues)

	if (!connection) {
		if (typeof (conn as MySQL.PoolConnection).release === 'function') (conn as MySQL.PoolConnection).release()
		else (conn as MySQL.Connection).destroy()
	}

	return response
}

async function set(table: string, data: { [key: string]: any }[]): Promise<MySQL.ResultSetHeader[]> {
	/**
	 * [!] Implementation Note
	 * - Add a grouping mapping of the received values to insert that ones with the same structure.
	 */
	const insertQueries = data.map(item => {
		const keys = Object.keys(item).map(k => `\`${k}\``).join(',')
		const values = Object.values(item)
		return query(`INSERT INTO \`${table}\` (${keys}) VALUES (${values.map(() => '?').join(',')})`, values)
	})

	const response = await Promise.all(insertQueries)
	const results = response.map(r => r[0] as MySQL.ResultSetHeader)
	return results
}

async function get(table: string, keys?: string[], filter?: (target: any) => boolean): Promise<[ MySQL.RowDataPacket[], MySQL.FieldPacket[] ]> {
	const queryString = !keys || keys.length === 0
		? `SELECT * FROM \`${table}\``
		: `SELECT ${keys.map(k => `\`${k}\``).join(', ')} FROM \`${table}\``

	const [ results, fields ] = await query(queryString) as [ MySQL.RowDataPacket[], MySQL.FieldPacket[] ]

	if (!filter) return [ results, fields ]
	else {
		const filteredResults = results.filter(v => filter(v))
		return [ filteredResults, fields ]
	}
}

async function edit(table: string, data: (data: any) => any, filter?: (target: any) => boolean): Promise<MySQL.ResultSetHeader[]> {
	const [ targets, fields ] = await get(table, [], filter)

	const primaryKey = fields.find(f => (f.flags & 2) !== 0)?.name
	// Removing PRIMARY KEYS (2) and UNIQUE INDEXES (4) using Bitwise Operator from Fields Flags.
	const editableColumns = fields.filter(f => (f.flags & 2) + (f.flags & 4) === 0).map(f => f.name)

	const updateQueries = targets.map(t => {
		const oldKeys = Object.keys(t)

		const updatedData = data(t)
		const newKeys = Object.keys(updatedData)
		const editableKeys = newKeys.filter(k => editableColumns.includes(k))
		
		const processedItem = Object.fromEntries(editableKeys.map(k => [k, processValue(updatedData[k], { json: true })]))
		
		const valuesString = editableKeys.map(k => `\`${k}\` = :${k}`).join(', ')
		
		const deleteConditions = (typeof primaryKey === 'string' ? [ primaryKey ] : oldKeys).map(k => `\`${k}\` = :old${k}`)
		
		const queryString = `UPDATE \`${table}\` SET ${valuesString} WHERE ${deleteConditions.join(' AND ')}`
		
		const selector = mapKeys(t, (v, k) => `old${k}`)
		const values = Object.assign(processedItem, selector)

		return query(queryString, values)
	})

	const response = await Promise.all(updateQueries)
	const results = response.map(r => r[0] as MySQL.ResultSetHeader)
	return results
}

async function del(table: string, filter?: (target: any) => boolean): Promise<MySQL.ResultSetHeader[]> {
	const [ targets ] = await get(table, [], filter)
	const deleteQueries = targets.map(t => {
		const keys = Object.keys(t)
		const deleteConditions = keys.map(k => `\`${k}\` = :${k}`)
		return query(`DELETE FROM \`${table}\` WHERE ${deleteConditions.join(' AND ')}`, t)
	})

	const response = await Promise.all(deleteQueries)
	const results = response.map(r => r[0] as MySQL.ResultSetHeader)
	return results
}

(async () => {
	const connection = await pool.getConnection()

	// const object = { id: 1, name: 'Angelo', email: 'angelo@example.com', preferences: { lang: 'en-US', theme: 'default' } }
	// console.log(mapKeys(object, (v, k) => `old${k}`))

	await get('users', [], i => i.id === 18).then(([[r]]) => console.log('Old User', r))
	await edit('users', i => Object.assign(i, { email: 'blankuser@example.net' }), i => i.id === 18).then(console.log)
	/**
	 * Your next step is to understand the logic inside edit function and create a global function
	 * to apply the same in any other filterable modification function
	 */
	await get('users', [], i => i.id === 18).then(([[r]]) => console.log('New User', r))

	// console.log('Test #1 - Query\n')

	// await query('CREATE TABLE IF NOT EXISTS `users` (`id` INT PRIMARY KEY AUTO_INCREMENT, `name` VARCHAR(255), `email` VARCHAR(255), `preferences` JSON, CONSTRAINT uc_id UNIQUE (`id`))', null, connection)
	/** await query(
		'INSERT INTO \`users\` (`id`, `name`, `email`, `preferences`) VALUES (?, ?, ?, ?), (?, ?, ?, ?), (?, ?, ?, ?)',
		[
			1, 'Jhon Doe', 'jhon@example.com', { lang: 'pt-BR', theme: 'default' },
			2, 'Jane Doe', 'jane@example.com', { lang: 'en-US', theme: 'default' },
			3, 'Bob Smith', 'bob@example.com', { lang: 'es-ES', theme: 'default' }
		],
		connection
	)
	**/

	// await set('users', [{ name: 'Blank User', email: 'blankuser@example.com', preferences: { lang: 'en-US', theme: 'default' } }]).then(console.log)
	// await edit('users', i => {
	// 	const preferences = Object.assign(i.preferences, { theme: 'dark' })
	// 	const item = Object.assign(i, { preferences })
	// 	return item
	// }, i => i.id === 18).then(console.log)
		
	// await get('users', [], i => i.id === 18).then(([r]) => console.log(r))
	// await del('users', i => i.id === 18).then(console.log)
	
	// await get('users', [], i => i.id === 18)
	// .then(async ([[u]]) => {
	// 	console.log('Target', u)
	// 	await connection.execute('DELETE FROM `users` WHERE `id` = :id AND `name` = :name AND `email` = :email AND `preferences` = :preferences', u)
	// 	.then(async ([r]) => {
	// 		console.log('DELETE', r)
	// 		await get('users', [], i => i.id === 18)
	// 		.then(([r]) => console.log('Id 18', r))
	// 	})
	// })


	connection.release()
})().catch(console.error).finally(() => console.log(`Done in ${(process.uptime() / 1000).toLocaleString('en-US', { maximumFractionDigits: 2 })}s`))
