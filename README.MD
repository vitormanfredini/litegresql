# Litegresql - A minimal PostgreSQL wire protocol server using in-memory SQLite for automated testing environments.

### Build and run:

```bash
yarn
tsc
node build/server.js
```

### Run tests:

```bash
# the server must be running before running this command
yarn test
```

### TODO:

1. keep track of foreign keys to manually validate them later
2. handle errors
3. handle transactions