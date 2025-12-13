const socketIO = require('socket.io');

module.exports = function (server) {
    const io = socketIO(server, {
        cors: {
            origin: '*',
        }
    });

    // Define your Socket.IO server logic
    io.on('connection', (socket) => {
        console.log('A client connected');
        socket.on('disconnect', () => {
            console.log('A client disconnected');
        });
    });

    return io;
};