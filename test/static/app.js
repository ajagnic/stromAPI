// JS client for socket


socket.on('lucy_on_couch_kody', function (data) {
    console.log(data);
    socket.send('lucy_on_couch_kody', data);
})
