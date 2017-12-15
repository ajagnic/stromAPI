// JS client for socket

socket = io.connect('http://' + document.domain + ':' + location.port);
socket.on('connect', function() {
    socket.emit('client_connected', {data: 'New client!'});
});

socket.on('message', function (data) {
    console.log('message from backend ' + data);
    socket.send('WOOOOO')
});

socket.on('alert', function (data) {
    alert('Alert Message!! ' + data);
});

socket.on('hello', function(data) {
  console.log('Hello!')
})

socket.on('event_detected', function(data) {
  console.log(data)
})

function json_button() {
    socket.send('{"message": "test"}');
}

function alert_button() {
    socket.emit('alert_button', 'Message from client!')
}

function hello_button() {
  socket.emit('hello_button', 'Message from client!')
}
