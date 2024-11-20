import { QueryType } from "./server";

enum FieldTypePostgres {
    INTEGER = 'INTEGER',
    BIGINT = 'BIGINT',
    REAL = 'REAL',
    DOUBLEPRECISION = 'DOUBLE PRECISION',
    TEXT = 'TEXT',
}

enum FieldTypeSqlite {
    INTEGER = 'INTEGER',
    REAL = 'REAL',
    TEXT = 'TEXT',
}

type ParsedQueryCommon = {
    type: QueryType,
    originalQuery: string
    translatedQuery: string
}

type ParsedQueryCreateTable = ParsedQueryCommon & {
    type: QueryType.createTable,
    table: string
    fields: CreateTableField[]
}

type CreateTableField = {
    name: string
    postgresFieldType: FieldTypePostgres
    sqliteFieldType: FieldTypeSqlite
    primaryKey: boolean
    autoIncrement: boolean
    nullable: boolean
}

type ParsedQuerySelect = ParsedQueryCommon & {
    type: QueryType.select,
}

type ParsedQueryInsert = ParsedQueryCommon & {
    type: QueryType.insert
    table: string
    returning: string | null
}

type ParsedQueryDelete = ParsedQueryCommon & {
    type: QueryType.delete
}

type ParsedQueryUpdate = ParsedQueryCommon & {
    type: QueryType.update
}

type ParsedQueryDrop = ParsedQueryCommon & {
    type: QueryType.drop
}

type FieldValuePair = {
    field: string
    value: string | number
}

type ParsedQuery = 
    | ParsedQueryCreateTable
    | ParsedQuerySelect
    | ParsedQueryInsert
    | ParsedQueryUpdate
    | ParsedQueryDelete
    | ParsedQueryDrop

export const parseQuery = (originalQuery: string): ParsedQuery => {

    // Remove any line breaks or excess whitespace
    const cleanedQuery = originalQuery.replace(/\s+/g, ' ').trim().replace(/;$/, '');

    const queryType = detectQueryType(originalQuery);

    if(queryType === QueryType.createTable){

        const tableMatch = cleanedQuery.match(/^CREATE TABLE (\w+)/i);
        if (!tableMatch) throw new Error("Invalid CREATE TABLE query");
        const table = tableMatch[1];

        // Match fields within parentheses
        const fieldsMatch = cleanedQuery.match(/\((.*)\)/);
        if (!fieldsMatch) throw new Error("Could not find field definitions");

        const fieldLines = fieldsMatch[1].split(',').map(field => field.trim());

        const fields: CreateTableField[] = []

        for(const fieldLine of fieldLines){

            const cleanedFieldLine = fieldLine.replace(/\s+/g, ' ').trim();

            const isAutoIncrement = (cleanedFieldLine.match(/\sSERIAL\b/gi) || []).length > 0;
            const isPrimaryKey = (cleanedFieldLine.match(/\sPRIMARY KEY\b/gi) || []).length > 0;

            const fieldName = cleanedFieldLine.split(' ')[0]

            const postgresFieldType = getPostgresFieldType(cleanedFieldLine)

            fields.push({
                autoIncrement: isAutoIncrement,
                primaryKey: isPrimaryKey,
                name: fieldName,
                nullable: getIsNullable(cleanedFieldLine),
                postgresFieldType: postgresFieldType,
                sqliteFieldType: getSqliteFieldType(postgresFieldType)
            });
        }

        return {
            type: QueryType.createTable,
            table: table,
            originalQuery: originalQuery,
            translatedQuery: generateCreateTableQuery(table, fields),
            fields: fields
        }
    }

    if(queryType === QueryType.select){
        return {
            type: QueryType.select,
            originalQuery: originalQuery,
            translatedQuery: cleanedQuery,
        }
    }

    if(queryType === QueryType.insert){
        const insertMatch = /INSERT INTO (\w+)\s*\((.*?)\)\s*VALUES\s*\((.*?)\)(?:\s*RETURNING\s*(.*?))?$/i.exec(cleanedQuery);
        if (!insertMatch) {
            throw new Error('Invalid INSERT query');
        }
        
        const [, table, fieldList, valueList, returningList] = insertMatch;

        return {
            type: QueryType.insert,
            table: table,
            originalQuery: originalQuery,
            translatedQuery: cleanedQuery,
            returning: returningList ? returningList : null
        }
    }

    if(queryType === QueryType.delete){
        return {
            type: QueryType.delete,
            originalQuery: originalQuery,
            translatedQuery: cleanedQuery,
        }
    }

    if(queryType === QueryType.update){
        return {
            type: QueryType.update,
            originalQuery: originalQuery,
            translatedQuery: cleanedQuery,
        }
    }

    if(queryType === QueryType.drop){
        return {
            type: QueryType.drop,
            originalQuery: originalQuery,
            translatedQuery: cleanedQuery,
        }
    }

    throw new Error(`parseQuery: Unsupported QueryType: ${queryType}`);

}

const generateCreateTableQuery = (table: string, fields: CreateTableField[]): string => {

    const fieldLines: string[] = []
    for(const field of fields){
        let nullOrNotNull = field.nullable ? 'NULL' : 'NOT NULL'
        let primaryKey = field.primaryKey ? 'PRIMARY KEY' : ''
        let sqliteFieldType = getSqliteFieldType(field.postgresFieldType)
        fieldLines.push(`${field.name} ${sqliteFieldType} ${nullOrNotNull} ${primaryKey}`);
    }

    return `CREATE TABLE ${table} ( ${fieldLines.join(", ")} );`
}

const getSqliteFieldType = (postgresFieldType: FieldTypePostgres): FieldTypeSqlite => {
    if(postgresFieldType == FieldTypePostgres.INTEGER) return FieldTypeSqlite.INTEGER;
    if(postgresFieldType == FieldTypePostgres.BIGINT) return FieldTypeSqlite.INTEGER;
    if(postgresFieldType == FieldTypePostgres.REAL) return FieldTypeSqlite.REAL;
    if(postgresFieldType == FieldTypePostgres.DOUBLEPRECISION) return FieldTypeSqlite.REAL;
    return FieldTypeSqlite.TEXT;
}

const getIsNullable = (createTableFieldLine: string) => {
    const isPrimaryKey = (createTableFieldLine.match(/\sPRIMARY KEY\b/gi) || []).length > 0;
    const hasNotNull = (createTableFieldLine.match(/\sNOT NULL\b/gi) || []).length > 0;
    const hasNull = (createTableFieldLine.match(/\sNULL\b/gi) || []).length > 0;

    if(hasNotNull){
        return false
    }

    const explicitNullable = hasNull && !hasNotNull;
    if(explicitNullable){
        return true
    }

    return !isPrimaryKey
}

const getPostgresFieldType = (createTableFieldLine: string): FieldTypePostgres => {
    const hasIntKeyword = (createTableFieldLine.match(/\sINT\b/gi) || []).length > 0;
    const hasIntegerKeyword = (createTableFieldLine.match(/\sINTEGER\b/gi) || []).length > 0;
    const hasSerialKeyword = (createTableFieldLine.match(/\sSERIAL\b/gi) || []).length > 0;
    if(hasIntKeyword || hasIntegerKeyword || hasSerialKeyword) return FieldTypePostgres.INTEGER

    const hasBigintKeyword = (createTableFieldLine.match(/\sBIGINT\b/gi) || []).length > 0;
    if(hasBigintKeyword) return FieldTypePostgres.BIGINT

    const hasDoublePrecisionKeyword = (createTableFieldLine.match(/\sDOUBLE PRECISION\b/gi) || []).length > 0;
    if(hasDoublePrecisionKeyword) return FieldTypePostgres.DOUBLEPRECISION

    const hasRealKeyword = (createTableFieldLine.match(/\sREAL\b/gi) || []).length > 0;
    if(hasRealKeyword) return FieldTypePostgres.REAL

    return FieldTypePostgres.TEXT
}

type ParsedCreateTableQuery = {
    table: string
    fields: string[]
}

const detectQueryType = (queryString: string): QueryType => {
    const trimmeUppercaseQueryString = queryString.trim().toUpperCase();
    for(const possibleQueryType of Object.values(QueryType)){
        if(trimmeUppercaseQueryString.startsWith(possibleQueryType)){
            return possibleQueryType
        }
    }

    throw new Error(`Couldn't detect query type from query: ${queryString}`)
}