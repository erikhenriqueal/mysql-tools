import dotenv from 'dotenv'
dotenv.config()

import { isPlainObject, mapValues } from 'lodash'
import MySQL from 'mysql2/promise'

export const { identify: identifyQuery } = require('sql-query-identifier')

export type MySQLResult = MySQL.RowDataPacket[] | MySQL.RowDataPacket[][] | MySQL.OkPacket | MySQL.OkPacket[] | MySQL.ResultSetHeader

export const pool: MySQL.Pool = MySQL.createPool({
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
 * - json: 'Whether you want to parse Object\'s and Array\'s to a JSON String.'
 * }
 * ```
 */
export function processValue(value: any, options?: { json?: boolean; }): any {
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
 * Parses your Query values by your SQL Query String.
 * @param queryString Query string of your request.
 * @param values Values you need to put in your query.
 * @param options Additional options.  
 * ---
 * - `options`.`force`: Uses bruteforce to prevent TypeError's by converting all invalid values to `null`. It's recommended to treat that errors by applying `NOT NULL` option on creating a new key.
 */
export function parseQueryStringValues(queryString: string, values?: any, options?: { force?: boolean }): any {
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
	
	if (placeholders.length === 1) {
		if (Array.isArray(values)) return [ processValue(values[0], { json: true }) ]
		else if (![ 'bigint', 'boolean', 'number', 'string' ].includes(typeof values)) return processValue(values, { json: true })
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

export async function query(string: string, values?: any, connection?: MySQL.Connection | MySQL.PoolConnection): Promise<[ MySQL.RowDataPacket[] | MySQL.RowDataPacket[][] | MySQL.OkPacket | MySQL.OkPacket[] | MySQL.ResultSetHeader, MySQL.FieldPacket[] ]> {
	const conn: MySQL.Connection | MySQL.PoolConnection = connection ? connection : await pool.getConnection()
	const parsedValues = parseQueryStringValues(string, values)
	const response = await conn.execute(string, parsedValues)

	if (!connection) {
		if (typeof (conn as MySQL.PoolConnection).release === 'function') (conn as MySQL.PoolConnection).release()
		else (conn as MySQL.Connection).destroy()
	}

	return response
}

export async function set(table: string, data: { [key: string]: any }[]): Promise<MySQL.ResultSetHeader[]> {
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

export async function get(table: string, keys?: string[], filter?: (target: any) => boolean): Promise<[ MySQL.RowDataPacket[], MySQL.FieldPacket[] ]> {
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

export async function edit(table: string, data: (data: any) => any, filter?: (target: any) => boolean): Promise<MySQL.ResultSetHeader[]> {
	const [ targets, fields ] = await get(table, [], filter)

	const primaryKey: string = fields.find(f => (f.flags & 2) !== 0)?.name
	// Removing PRIMARY KEYS (2) and UNIQUE INDEXES (4) using Bitwise Operator from Fields Flags.
	const uniqueKeys: string[] = fields.filter(f => (f.flags & 4) !== 0).map(f => f.name)

	const updateQueries = targets.map(t => {
		const targetKeys: string[] = Object.keys(t)

		const updatedData = data(new Object(t))
		const editableKeys: string[] = targetKeys.filter(k => ![primaryKey, ...uniqueKeys].includes(k))
		
		const processedEntries: [string, any][] = editableKeys.map(k => [k, processValue(updatedData[k], { json: true })])
		const processedItem: { [k: string]: any } = Object.fromEntries(processedEntries)
		
		const valuesString = editableKeys.map(k => `\`${k}\` = :${k}`).join(', ')

		const selectorKeys: string[] = []
		if (typeof primaryKey === 'string') selectorKeys.push(primaryKey)
		else {
			const keys: string[] = uniqueKeys.length > 0 ? uniqueKeys : targetKeys
			selectorKeys.push(...keys)
		}
		const deleteCondition: string = selectorKeys.map(k => `\`${k}\` = :old${k}`).join(' AND ')
		
		const selectorEntries = selectorKeys.map(k => [`old${k}`, t[k]])
		const selectorObject = Object.fromEntries(selectorEntries)
		
		const queryString = `UPDATE \`${table}\` SET ${valuesString} WHERE ${deleteCondition}`
		const values = Object.assign(processedItem, selectorObject)
		
		return query(queryString, values)
	})

	const response = await Promise.all(updateQueries)
	const results = response.map(r => r[0] as MySQL.ResultSetHeader)
	return results
}

export async function del(table: string, filter?: (target: any) => boolean): Promise<MySQL.ResultSetHeader[]> {
	const [ targets, fields ] = await get(table, [], filter)

	const primaryKey: string = fields.find(f => (f.flags & 2) !== 0)?.name
	// Removing PRIMARY KEYS (2) and UNIQUE INDEXES (4) using Bitwise Operator from Fields Flags.
	const uniqueKeys: string[] = fields.filter(f => (f.flags & 4) !== 0).map(f => f.name)

	const deleteQueries = targets.map(t => {
		const selectorKeys: string[] = []
		if (typeof primaryKey === 'string') selectorKeys.push(primaryKey)
		else {
			const keys: string[] = uniqueKeys.length > 0 ? uniqueKeys : Object.keys(t)
			selectorKeys.push(...keys)
		}

		const deleteCondition: string = selectorKeys.map(k => `\`${k}\` = :${k}`).join(' AND ')

		const entries: [string, any][] = selectorKeys.map(k => [k, t[k]])
		const values: { [k: string]: any } = Object.fromEntries(entries)
		
		return query(`DELETE FROM \`${table}\` WHERE ${deleteCondition}`, values)
	})

	const response = await Promise.all(deleteQueries)
	const results = response.map(r => r[0] as MySQL.ResultSetHeader)
	return results
}