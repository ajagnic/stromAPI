// JS client for socket


socket.on('lucy_on_couch_kody', function (data) {
    console.log(data);
    socket.emit('lucy_on_couch_kody2', json.dumps(data));
})
