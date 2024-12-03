import { execSync } from 'child_process';
import { PrismaClient } from '@prisma/client';

describe('Prisma with migrations in tests', () => {
  let prisma: PrismaClient;
  
  beforeAll(() => {
    execSync('npx prisma generate --schema=tests/schema.prisma', { stdio: 'inherit' });
    execSync('npx prisma migrate deploy --schema=tests/schema.prisma', { stdio: 'inherit' });

    prisma = new PrismaClient()
  });

  afterAll(async () => {
    console.log('Cleaning up...');
    await prisma.$executeRawUnsafe(`DROP TABLE IF EXISTS "User";`);
    await prisma.$disconnect();
  });

  test('should insert and retrieve a row correctly', async () => {
    const insertedUser = await prisma.user.create({
      data: {
        name: 'Alice',
        age: 25,
      },
    });

    expect(insertedUser).toMatchObject({
      id: expect.any(Number),
      name: 'Alice',
      age: 25,
    });

    const fetchedUser = await prisma.user.findUnique({
      where: { id: insertedUser.id },
    });

    expect(fetchedUser).toMatchObject({
      id: insertedUser.id,
      name: 'Alice',
      age: 25,
    });
  });
});