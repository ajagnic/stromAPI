// JS client for socket
socket = io.connect('http://' + document.domain + ':' + location.port);

socket.on('lucy_on_couch_kody', function (data) {
    console.log(data);
    socket.send( json.dumps(data));
})
