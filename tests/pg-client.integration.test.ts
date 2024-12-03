import { Client } from 'pg';

const createClient = async () => {
  const client = new Client({
    user: 'your_user',
    host: 'localhost',
    database: 'test_db',
    password: 'your_password',
    port: 5432,
  });
  await client.connect();
  return client;
};

describe('PostgreSQL Mimic Integration Tests', () => {
  let client: Client;

  beforeAll(async () => {
    client = await createClient();
  });

  afterAll(async () => {
    await client.end();
  });

  it('should create a table', async () => {
    const result = await client.query(`
      CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        age INT
      );
    `);
    expect(result.command).toBe('CREATE');
  });

  // it('should insert 2 rows into the table', async () => {
  //   const result = await client.query(`
  //     INSERT INTO users 
  //     (name, age) 
  //     VALUES
  //     ('Alice', 30)
  //     RETURNING id;
  //   `);
  //   expect(result.rowCount).toBe(1);
  //   expect(result.command).toBe('INSERT');
  //   expect(result.rows[0].id).toBeGreaterThan(0);

  //   const result2 = await client.query(`
  //     INSERT INTO users 
  //     (name, age) 
  //     VALUES
  //     ('Bob', 13)
  //     RETURNING id;
  //   `);
  //   expect(result2.rowCount).toBe(1);
  //   expect(result2.command).toBe('INSERT');
  //   expect(result2.rows[0].id).toBeGreaterThan(0);
  // });

  // it('should select rows from the table', async () => {
  //   const result = await client.query(`SELECT * FROM users WHERE name = 'Alice';`);
  //   expect(result.command).toBe('SELECT');
  //   expect(result.rows.length).toBe(1);
  //   expect(result.rows[0].name).toBe('Alice');
  //   expect(result.rows[0].age).toBe(30);
  // });

  // it('should update a row in the table', async () => {
  //   const result = await client.query(`UPDATE users SET age = 31 WHERE name = 'Alice';`);
  //   expect(result.command).toBe('UPDATE');
  //   expect(result.rowCount).toBe(1);

  //   const selectResult = await client.query(`SELECT age FROM users WHERE name = 'Alice';`);
  //   expect(selectResult.rows[0].age).toBe(31);
  // });

  // it('should COUNT rows', async () => {
  //   const result = await client.query(`SELECT COUNT(*) FROM users;`);
  //   expect(result.command).toBe('SELECT');
  //   expect(result.rowCount).toBe(1);
  //   expect(result.rows[0]['COUNT(*)']).toBe(2)
  // });

  // it('should SUM ages of all rows', async () => {
  //   const result = await client.query(`SELECT SUM(age) FROM users;`);
  //   expect(result.command).toBe('SELECT');
  //   expect(result.rowCount).toBe(1);
  //   expect(result.rows[0]['SUM(age)']).toBe(44)
  // });

  // it('should delete a row from the table', async () => {
  //   const result = await client.query(`DELETE FROM users WHERE name = 'Alice';`);
  //   expect(result.command).toBe('DELETE');
  //   expect(result.rowCount).toBe(1);

  //   const selectResult = await client.query(`SELECT * FROM users WHERE name = 'Alice';`);
  //   expect(selectResult.rows.length).toBe(0);
  // });

  it('should drop the table', async () => {
    const result = await client.query(`DROP TABLE users;`);
    expect(result.command).toBe('DROP');
  });

  // it('should create a table, insert, update, delete and select correctly.', async () => {
  //   const resultCreateTable = await client.query(`
  //     CREATE TABLE testing (
  //       id SERIAL PRIMARY KEY,
  //       name TEXT NOT NULL,
  //       age INT
  //     );
  //   `);

  //   const data = [
  //     {
  //       name: 'Clara',
  //       age: 29
  //     },
  //     {
  //       name: 'Adam',
  //       age: 62
  //     },
  //     {
  //       name: 'Robertinho da Silva',
  //       age: 25
  //     }
  //   ]

  //   for(const dataToInsert of data){
  //     const result = await client.query(`
  //       INSERT INTO testing (name, age)
  //       VALUES ('${dataToInsert.name}', ${dataToInsert.age})
  //       RETURNING id;
  //     `);
  //     expect(result.rowCount).toBe(1);
  //   }

  //   const resultSelect = await client.query(`SELECT * FROM testing;`);
  //   expect(resultSelect.rows.length).toBe(3)

  //   for(let c=0;c<resultSelect.rows.length; c++){
  //     expect(resultSelect.rows[c]['name']).toBe(data[c]['name']);
  //     expect(resultSelect.rows[c]['age']).toBe(data[c]['age']);
  //   }

  //   const resultUpdate = await client.query(`UPDATE testing SET name = 'George' WHERE id = '2';`);

  //   const resultSelect2 = await client.query(`SELECT * FROM testing WHERE id = '2';`);
  //   expect(resultSelect2.rows[0]['name']).toBe('George')

  //   const resultDelete = await client.query(`DELETE FROM testing WHERE name = 'Robertinho da Silva';`);
  //   expect(resultDelete.rowCount).toBe(1)

  //   const resultSelect3 = await client.query(`SELECT * FROM testing;`);
  //   expect(resultSelect3.rows.length).toBe(2)

  //   const resultDrop = await client.query(`DROP TABLE testing;`);
  //   expect(resultDrop.command).toBe('DROP');
  // });
});