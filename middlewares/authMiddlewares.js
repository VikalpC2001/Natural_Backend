const jwt = require('jsonwebtoken');
const asyncHandler = require("express-async-handler");
const pool = require('../database');

const protect = asyncHandler(async (req, res, next) => {
    let token
    if (
        req.headers.authorization &&
        req.headers.authorization.startsWith("Bearer")
    ) {
        try {
            token = req.headers ? req.headers.authorization.split(" ")[1] : null;

            //decodes token id
            const decoded = jwt.verify(token, process.env.JWT_SECRET);

            //SQL Code 
            const sql_querry_getdetailsById = `SELECT * FROM user_details WHERE userId = "${decoded.id.id}"`;
            pool.query(sql_querry_getdetailsById, (err, data) => {
                if (err) {
                    process.exit(1);
                    // return res.send(err);
                }
                if (data[0] && data[0].userRights && data[0].userRights == decoded.id.rights) {
                    next();
                } else {
                    res.status(401).send("User Not Found");
                }
            })
        } catch (error) {
            res.status(401).send("Not authorized, token failed");
        }
    }
    if (!token) {
        res.status(401).send("You Are Not authorized, Token Not Found");
    }
});

module.exports = { protect };