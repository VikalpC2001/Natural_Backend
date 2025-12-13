const express = require('express');
const bodyparser = require('body-parser');
const dotenv = require('dotenv').config();
const cors = require('cors');
const { notFound, erroHandler } = require('./middlewares/errorMiddleware');
const createSocketServer = require('./appSocket');
const http = require('http')
const fs = require('fs');
const app = express()
const port = process.env.PORT || 8009;
const userrouter = require('./routs/userRouts/user.routs');
const branchrouter = require('./routs/branchRouts/branch.routs');
const menuItemrouter = require('./routs/menuItemRouts/item.routs');
const billingrouter = require('./routs/billingRouts/billing.routs');

// app.use(cors({
//   credentials: true,
//   origin: [
//     "http://localhost:3000",
//     "http://localhost:5000"
//   ],
//   exposedHeaders: ["set-cookie"],
// }));

app.use(cors());
app.use((req, res, next) => {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.header(
        "Access-Control-Allow-Headers",
        "Origin, X-Requested-With, Content-Type, Accept"
    );
    next();
});

const server = http.createServer(app);

const io = createSocketServer(server);

app.use((req, res, next) => {
    req.io = io;
    next();
});

app.use(bodyparser.urlencoded({ extended: false }))
app.use(bodyparser.json())

app.use('/userrouter', userrouter);
app.use('/branchrouter', branchrouter);
app.use('/menuItemrouter', menuItemrouter);
app.use('/billingrouter', billingrouter);


app.use(notFound);
app.use(erroHandler);

server.listen(port, () => console.log(`Connection successful ${port}`));