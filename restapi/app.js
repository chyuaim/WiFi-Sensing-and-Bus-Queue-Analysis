const express = require('express');
const app = express();
const mongoose = require('mongoose');
const cors = require('cors');
const path = require('path');
const https = require('https');
const http = require('http');
const fs = require('fs');

const pplno_south_Route = require('./routes/pplno_south');
const qtd_current_91M_Route = require('./routes/qtd_current_91M');
const qtd_current_11_Route = require('./routes/qtd_current_11');
const qtd_current_104_Route = require('./routes/qtd_current_104');
const qtd_current_91P_Route = require('./routes/qtd_current_91P');
const qtd_boarded_91M_Route = require('./routes/qtd_boarded_91M');
const qtd_boarded_11_Route = require('./routes/qtd_boarded_11');
const qtd_boarded_104_Route = require('./routes/qtd_boarded_104');
const qtd_boarded_91P_Route = require('./routes/qtd_boarded_91P');
const pplno_north_Route = require('./routes/pplno_north');
const qtd_current_North_Route = require('./routes/qtd_current_north');
const qtd_boarded_North_Route = require('./routes/qtd_boarded_north');
const port = 3000;

// var options = {
//       ca: fs.readFileSync(path.join(__dirname,'ca_bundle.crt')),
//       cert: fs.readFileSync(path.join(__dirname,'certificate.crt')),
//       key: fs.readFileSync(path.join(__dirname,'private.key'))
//     };

//middleware
app.use(cors());
app.use(express.json());

//routes
app.use('/pplno_south', pplno_south_Route);
app.use('/qtd_current_91M', qtd_current_91M_Route);
app.use('/qtd_current_11', qtd_current_11_Route);
app.use('/qtd_current_104', qtd_current_104_Route);
app.use('/qtd_current_91P', qtd_current_91P_Route);
app.use('/qtd_boarded_91M', qtd_boarded_91M_Route);
app.use('/qtd_boarded_11', qtd_boarded_11_Route);
app.use('/qtd_boarded_104', qtd_boarded_104_Route);
app.use('/qtd_boarded_91P', qtd_boarded_91P_Route);
app.use('/pplno_north', pplno_north_Route);
app.use('/qtd_current_north', qtd_current_North_Route);
app.use('/qtd_boarded_north', qtd_boarded_North_Route);

app.get('/', (req,res) => {
    res.sendFile(path.join(__dirname,'index/index.html'));
});

mongoose.connect('mongodb://localhost:27017/fyp_2021_busq', 
    { useNewUrlParser: true, useUnifiedTopology: true }, () => {
    console.log('connected to DB');
});

// var server = https.createServer(options, app);
var server = http.createServer(app)
server.listen(port);
