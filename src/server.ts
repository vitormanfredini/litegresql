import net from 'net';
import sqlite3 from 'sqlite3';
import { parseQuery } from './parser';

const db: sqlite3.Database = new sqlite3.Database(':memory:');

const store: {[key: string]: any} = {};

let commandCompleteMessageQueue: Buffer[] = []

const addCommandCompleteMessageToQueue = (message: Buffer): void => {
    commandCompleteMessageQueue.push(message)
}

const sendMessagesFromCommandCompleteMessageQueueAndClear = (socket: net.Socket) => {
    for(const message of commandCompleteMessageQueue){
        socket.write(message);
    }
    commandCompleteMessageQueue = []
}

enum PostgresDataType {
    INT4 = 23,    // INTEGER
    INT8 = 20,    // BIGINT
    FLOAT4 = 700, // REAL
    FLOAT8 = 701, // DOUBLE PRECISION
    TEXT = 25,    // TEXT
}

export enum MessageType {
    AuthenticationOk = 'R',
    ParameterStatus = 'S',
    ReadyForQuery = 'Z',
    BackendKeyData = 'K',
    Query = 'Q',
    CommandComplete = 'C',
    Sync = 'S',
    RowDescription = 'T',
    DataRow = 'D',
    Terminate = 'X',
}

export enum ReadyForQueryComplement {
    Idle = 'I',
    InTransaction = 'T',
    FailedTransaction = 'E',
}

export enum QueryType {
    createTable = 'CREATE TABLE',
    select = 'SELECT',
    insert = 'INSERT',
    update = 'UPDATE',
    delete = 'DELETE',
    drop = 'DROP',
}

const MESSAGE_LENGTH_LENGTH = 4

type KeyValueParameters = {
    key: string,
    value: string
}

const makeMessage = (messageType: MessageType, data: Buffer): Buffer => {
    const length = Buffer.alloc(MESSAGE_LENGTH_LENGTH);
    length.writeUInt32BE(MESSAGE_LENGTH_LENGTH + data.length + 1);
    return Buffer.concat([Buffer.from(messageType), length, data, Buffer.from([0])]);
};

const makeParametersMessage = (parameters: KeyValueParameters[]): Buffer => {
    const initialParametersMessages: Buffer[] = parameters.map(param => makeMessage(MessageType.ParameterStatus, Buffer.from(`${param.key}\0${param.value}\0`, 'utf-8')));
    return Buffer.concat(initialParametersMessages);
}

const storeClientParameters = (data: Buffer): void => {
    const messageLength = data.readUInt32BE(0);
    const protocolVersion = data.readUInt32BE(4);

    if (protocolVersion !== 0x00030000) {
        throw new Error("Unsupported protocol version.");
    }

    const parameters: Record<string, string> = {};
    let offset = 8;

    while (offset + 1 < messageLength) {
        const keyEnd = data.indexOf(0, offset); // Find null terminator for key
        const key = data.toString('utf8', offset, keyEnd);
        offset = keyEnd + 1;

        const valueEnd = data.indexOf(0, offset); // Find null terminator for value
        const value = data.toString('utf8', offset, valueEnd);
        offset = valueEnd + 1;

        parameters[key] = value;
    }

    store['protocolVersion'] = protocolVersion;
    store['parameters'] = parameters;
}

const handleStartupMessage = (socket: net.Socket, data: Buffer): void => {

    storeClientParameters(data)

    // authenticationOk message is all zeroes, so only allocating the buffer is enough
    socket.write(makeMessage(MessageType.AuthenticationOk, Buffer.alloc(4)));

    socket.write(makeParametersMessage([
        {
            key: 'server_version',
            value: '13.3',
        },
        {
            key: 'client_encoding',
            value: 'UTF8',
        },
        {
            key: 'TimeZone',
            value: 'UTC',
        },
    ]));

    // Backend Key Data
    const processIdBuffer = Buffer.alloc(4);
    processIdBuffer.writeUInt32BE(12345);
    const secretKeyBuffer = Buffer.alloc(4);
    secretKeyBuffer.writeUInt32BE(54321);
    socket.write(makeMessage(MessageType.BackendKeyData, Buffer.concat([processIdBuffer, secretKeyBuffer])));

    // Ready For Query
    socket.write(makeMessage(MessageType.ReadyForQuery, Buffer.from(ReadyForQueryComplement.Idle)));
}

function dbAll(sqlQuery: string, params: any[] = []): Promise<Record<string, any>[]> {
    return new Promise((resolve, reject) => {
        db.all(sqlQuery, params, (err, rows) => {
            if (err) {
                reject(new Error(`SQL error: ${err.message}`));
            } else {
                resolve(rows as Record<string, any>[]);
            }
        });
    });
}

// Types for column information
interface ColumnDescription {
    name: string;
    tableOid: number;
    columnAttrNumber: number;
    dataTypeOid: number;
    dataTypeSize: number;
    typeModifier: number;
    formatCode: number;
}

const makeRowDescriptionMessage = (columns: ColumnDescription[]): Buffer => {
    // First create buffer for number of fields (Int16)
    const fieldCountBuffer = Buffer.alloc(2);
    fieldCountBuffer.writeInt16BE(columns.length);
    
    // Create field description buffers
    const columnBuffers = columns.map(column => {
        // For each column we need:
        // - name (String + null terminator)
        // - tableOID (Int32)
        // - columnAttrNumber (Int16)
        // - dataTypeOid (Int32)
        // - dataTypeSize (Int16)
        // - typeModifier (Int32)
        // - formatCode (Int16)
        
        const nameBuffer = Buffer.from(column.name + '\0');
        const columnBuffer = Buffer.alloc(18); // 4 + 2 + 4 + 2 + 4 + 2 = 18 bytes
        
        let offset = 0;
        columnBuffer.writeInt32BE(column.tableOid, offset);
        offset += 4;
        columnBuffer.writeInt16BE(column.columnAttrNumber, offset);
        offset += 2;
        columnBuffer.writeInt32BE(column.dataTypeOid, offset);
        offset += 4;
        columnBuffer.writeInt16BE(column.dataTypeSize, offset);
        offset += 2;
        columnBuffer.writeInt32BE(column.typeModifier, offset);
        offset += 4;
        columnBuffer.writeInt16BE(column.formatCode, offset);
        
        return Buffer.concat([nameBuffer, columnBuffer]);
    });
    
    // Combine field count with all column descriptions
    return makeMessage(
        MessageType.RowDescription,
        Buffer.concat([fieldCountBuffer, ...columnBuffers])
    );
};

const makeDataRowMessage = (values: (string | number | null)[]): Buffer => {
    // First create buffer for number of columns (Int16)
    const columnCountBuffer = Buffer.alloc(2);
    columnCountBuffer.writeInt16BE(values.length);
    
    // Create value buffers
    const valueBuffers = values.map(value => {
        if (value === null) {
            // For null values, we write -1 as the length (Int32)
            const lengthBuffer = Buffer.alloc(4);
            lengthBuffer.writeInt32BE(-1);
            return lengthBuffer;
        }
        
        // Convert value to string
        const strValue = String(value);
        const valueBuffer = Buffer.from(strValue);
        
        // Create length buffer (Int32)
        const lengthBuffer = Buffer.alloc(4);
        lengthBuffer.writeInt32BE(valueBuffer.length);
        
        return Buffer.concat([lengthBuffer, valueBuffer]);
    });
    
    // Combine column count with all value buffers
    return makeMessage(
        MessageType.DataRow,
        Buffer.concat([columnCountBuffer, ...valueBuffers])
    );
};

const handleQueryMessage = async (socket: net.Socket, data: Buffer): Promise<void> => {

    const messageLength = data.readUInt32BE(1);
    const receivedQueryString = data.subarray(5, messageLength - 1).toString('utf-8');
    const parsedQuery = parseQuery(receivedQueryString);
    // console.log('parsedQuery:', parsedQuery)

    let rows;
    try {
        rows = await dbAll(parsedQuery.translatedQuery)
        // console.log(rows)
    } catch (error) {
        console.error('SQL error:', (error as Error).message);
        return;
    }

    const isInsertReturning = parsedQuery.type === QueryType.insert && parsedQuery.returning !== null;
    if(parsedQuery.type === QueryType.select || isInsertReturning){
        
        const columns: ColumnDescription[] = Object.keys(rows[0] || {}).map((name, index) => ({
            name,
            tableOid: 0,
            columnAttrNumber: index + 1,
            dataTypeOid: typeof rows[0][name] === 'number' ? PostgresDataType.INT4 : PostgresDataType.TEXT,
            dataTypeSize: -1,
            typeModifier: -1,
            formatCode: 0
        }));
        
        socket.write(makeRowDescriptionMessage(columns));
        
        for (const row of rows) {
            const values = Object.values(row);
            socket.write(makeDataRowMessage(values));
        }

        if(!isInsertReturning){

            const commandCompleteMessage = makeMessage(
                MessageType.CommandComplete,
                Buffer.from(`SELECT ${rows.length}`)
            );
            socket.write(commandCompleteMessage);

            const readyForQueryMessage = makeMessage(
                MessageType.ReadyForQuery, 
                Buffer.from(ReadyForQueryComplement.Idle)
            );
            socket.write(readyForQueryMessage);
        }
        
    }

    const queryTypesRespondWithCommandCompleteOnly: QueryType[] = [
        QueryType.createTable,
        QueryType.drop,
    ]

    if(queryTypesRespondWithCommandCompleteOnly.includes(parsedQuery.type)){
        const commandCompleteMessage = makeMessage(MessageType.CommandComplete, Buffer.from(parsedQuery.type));
        socket.write(commandCompleteMessage);

        const readyForQueryMessage = makeMessage(MessageType.ReadyForQuery, Buffer.from(ReadyForQueryComplement.Idle));
        socket.write(readyForQueryMessage);
    }

    const queryTypesRespondWithChangedRows: QueryType[] = [
        QueryType.insert,
        QueryType.delete,
        QueryType.update
    ]

    if(queryTypesRespondWithChangedRows.includes(parsedQuery.type)){

        const totalChangesSql = 'SELECT changes()';
        const totalChangesRows = await dbAll(totalChangesSql)

        const rowsChanged = totalChangesRows[0]['changes()'];
        const commandCompleteMessage = makeMessage(MessageType.CommandComplete, Buffer.from(`${parsedQuery.type} 0 ${rowsChanged}`));
        socket.write(commandCompleteMessage);

        const readyForQueryMessage = makeMessage(MessageType.ReadyForQuery, Buffer.from(ReadyForQueryComplement.Idle));
        socket.write(readyForQueryMessage);
    }

}

const handleSyncMessage = (socket: net.Socket, data: Buffer): void => {
    sendMessagesFromCommandCompleteMessageQueueAndClear(socket)

    const readyForQueryMessage = makeMessage(MessageType.ReadyForQuery, Buffer.from(ReadyForQueryComplement.Idle));
    socket.write(readyForQueryMessage);
}

const start = (host: string, port: number) => {
    const server = net.createServer((socket) => {
        console.log(`Client connected from ${socket.remoteAddress}`);

        socket.on('data', async (data: Buffer) => {

            // console.log('Received:', data);

            const isStartupMessage = data.length >= 8 && data.readUInt32BE(4) === 0x00030000;
            if(isStartupMessage){
                handleStartupMessage(socket, data)
                return
            }

            const isQueryMessage = String.fromCharCode(data[0]) === MessageType.Query;
            if(isQueryMessage){
                await handleQueryMessage(socket, data)
                return
            }

            const isSyncMessage = String.fromCharCode(data[0]) === MessageType.Sync;
            if(isSyncMessage){
                handleSyncMessage(socket, data)
                return
            }

            const isTerminateMessage = String.fromCharCode(data[0]) === MessageType.Terminate;
            if(isTerminateMessage){
                return
            }

            console.log('Unsupported message:')
            console.log(data)

        });

        socket.on('end', () => {
            console.log('Client disconnected');
        });
    });

    server.listen(port, host, () => {
        console.log(`Server listening on ${host}:${port}`);
    });
}

start('localhost', 5432);