const express = require('express');
const { createServer } = require("node:http");
const { join } = require('node:path');
const { Server } = require('socket.io');



const sqlite3 = require('sqlite3').verbose();
const { open } = require('sqlite');
const {availableParallelism} = require('node:os');
const cluster = require('node:cluster');
const { createAdapter, setupPrimary } = require('@socket.io/cluster-adapter');

if (cluster.isPrimary){
    const numCPUs = availableParallelism();
    for (let i = 0; i < numCPUs; i++) {
        cluster.fork({
            PORT: 3001 + i
        });
    }
    return setupPrimary();
}
/************************ MODULE ********************/


async function  main() {
    const app = express();
    const host = "192.168.1.199";
    const server = createServer(app);
    const io = new Server(server,{
        connectionStateRecovery:{},
        adapter: createAdapter()
    });
    const db = await open({
        filename: 'chat.db',
        driver: sqlite3.Database
    });

    //await db.exec("DROP TABLE messages");

    await db.exec(`
        CREATE TABLE IF NOT EXISTS  messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_offset TEXT UNIQUE,
            content TEXT
        );
    `);

    app.get('/', (req, res) => {
        res.sendFile(join(__dirname, 'index.html'));
    })

    io.on('connection', async (socket) => {
        console.log('a user connected')
        socket.on('chat message', async (message, clientOffset, callback) => {
            let result;
            try{
                result = await db.run('INSERT INTO messages ( content, client_offset ) VALUES ( ?,? )',message, clientOffset);
            }catch (e){
                console.error(e);
                if (e.errno === 19){
                    callback();
                }else{

                }
                return;
            }
            io.emit('chat message', message, result.lastID);
            callback();
        });


        if (!socket.recovered){
            try{
                await db.each("SELECT id, content FROM messages WHERE id > ?",
                    [socket.handshake.auth.serverOffset || 0],
                    (_err, row) => {
                    socket.emit('chat message', row.content, row.id)
                    });
            }catch (e){
                console.error(e);
            }
        }

        socket.on('disconnect', (socket) => {
            console.error(`user ${socket} disconnected`);
        });
    })
    const _port  = process.env.PORT;
    server.listen(_port, host, () => {
        console.log(`Server is running at http://${host}:${_port}`);
    });
}
main().then(r => {
    console.log(r);
})