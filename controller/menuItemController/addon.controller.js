const pool = require('../../database');
const jwt = require("jsonwebtoken");

// Get addons List

const getAddOnsList = async (req, res) => {
    try {
        const page = req.query.page;
        const numPerPage = req.query.numPerPage;
        const skip = (page - 1) * numPerPage;
        const limit = skip + ',' + numPerPage;
        sql_querry_getCountDetails = `SELECT count(*) as numRows FROM item_addons_data`;
        pool.query(sql_querry_getCountDetails, (err, rows, fields) => {
            if (err) {
                console.error("An error occurred in SQL Queery", err);
                return res.status(500).send('Database Error');
            } else {
                const numRows = rows[0].numRows;
                const numPages = Math.ceil(numRows / numPerPage);
                const sql_query_getDetails = `SELECT addonsId ,groupId, addonsName, addonsGujaratiName, price, isActive FROM item_addons_data LIMIT ${limit}`;
                pool.query(sql_query_getDetails, (err, rows, fields) => {
                    if (err) {
                        console.error("An error occurred in SQL Queery", err);
                        return res.status(500).send('Database Error');;
                    } else {
                        if (numRows === 0) {
                            const rows = [{
                                'msg': 'No Data Found'
                            }]
                            return res.status(200).send({ rows, numRows });
                        } else {
                            return res.status(200).send({ rows, numRows });
                        }
                    }
                });
            }
        })
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).json('Internal Server Error');
    }
}

// Add addons AddOns

const addAddOnsData = async (req, res) => {
    try {
        let token;
        token = req.headers ? req.headers.authorization.split(" ")[1] : null;
        if (token) {
            const decoded = jwt.verify(token, process.env.JWT_SECRET);
            const rights = decoded.id.rights;
            if (rights == 1) {
                const uid1 = new Date();
                const addonsId = String("addOns_" + uid1.getTime());

                const data = {
                    groupId: req.body.groupId ? req.body.groupId : null,
                    addonsName: req.body.addonsName ? req.body.addonsName.trim() : null,
                    addonsGujaratiName: req.body.addonsGujaratiName ? req.body.addonsGujaratiName.trim() : null,
                    price: req.body.price ? req.body.price : 0,
                    isActive: req.body.isActive ? req.body.isActive : false
                }
                if (!data.groupId || !data.addonsName || !data.price) {
                    return res.status(400).send("Please Fill All Fields....!");
                } else {
                    pool.query(`SELECT addonsName FROM item_addons_data WHERE addonsName = '${data.addonsName}'`, function (err, row) {
                        if (err) {
                            console.error("An error occurred in SQL Queery", err);
                            return res.status(500).send('Database Error');
                        }
                        if (row && row.length) {
                            return res.status(400).send('AddOns is Already In Use');
                        } else {
                            const sql_querry_addCategory = `INSERT INTO item_addons_data (addonsId, groupId, addonsName, addonsGujaratiName, price, isActive)  
                                                            VALUES ('${addonsId}', '${data.groupId}', '${data.addonsName}', '${data.addonsGujaratiName}', ${data.price}, ${data.isActive})`;
                            pool.query(sql_querry_addCategory, (err, data) => {
                                if (err) {
                                    console.error("An error occurred in SQL Queery", err);
                                    return res.status(500).send('Database Error');
                                }
                                return res.status(200).send("AddOns Added Successfully");
                            })
                        }
                    })
                }
            } else {
                return res.status(400).send('You are Not Authorised');
            }
        } else {
            return res.status(404).send('Please Login First...!');
        }
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).send('Internal Server Error');
    }
}

// Remove addons AddOns

const removeAddOnsData = async (req, res) => {
    try {
        let token;
        token = req.headers ? req.headers.authorization.split(" ")[1] : null;
        if (token) {
            const decoded = jwt.verify(token, process.env.JWT_SECRET);
            const rights = decoded.id.rights;
            if (rights == 1) {
                const addonsId = req.query.addonsId;
                req.query.addonsId = pool.query(`SELECT addonsId FROM item_addons_data WHERE addonsId = '${addonsId}'`, (err, row) => {
                    if (err) {
                        console.error("An error occurred in SQL Queery", err);
                        return res.status(500).send('Database Error');
                    }
                    if (row && row.length) {
                        const sql_querry_removedetails = `DELETE FROM item_addons_data WHERE addonsId = '${addonsId}'`;
                        pool.query(sql_querry_removedetails, (err, data) => {
                            if (err) {
                                console.error("An error occurred in SQL Queery", err);
                                return res.status(500).send('Database Error');
                            }
                            return res.status(200).send("AddOns Deleted Successfully");
                        })
                    } else {
                        return res.send('AddonsId Not Found');
                    }
                })
            } else {
                return res.status(400).send('You are Not Authorised');
            }
        } else {
            return res.status(404).send('Please Login First...!');
        }
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).send('Internal Server Error');
    }
}

// Update addons AddOns

const updateAddOnsData = async (req, res) => {
    try {
        const data = {
            addonsId: req.body.addonsId ? req.body.addonsId : null,
            groupId: req.body.groupId ? req.body.groupId : null,
            addonsName: req.body.addonsName ? req.body.addonsName.trim() : null,
            addonsGujaratiName: req.body.addonsName ? req.body.addonsGujaratiName.trim() : null,
            price: req.body.price ? req.body.price : 0,
            isActive: req.body.isActive ? req.body.isActive : false
        }
        if (!data.addonsId || !data.groupId || !data.addonsName || !data.price) {
            return res.status(400).send("Please Fill All Fields....!");
        }
        const sql_querry_updatedetails = `UPDATE 
                                            item_addons_data 
                                          SET 
                                            groupId = '${data.groupId}',
                                            addonsName = '${data.addonsName}',
                                            addonsGujaratiName = '${data.addonsGujaratiName}',
                                            price = ${data.price},
                                            isActive = ${data.isActive}
                                          WHERE addonsId = '${data.addonsId}'`;
        pool.query(sql_querry_updatedetails, (err, data) => {
            if (err) {
                console.error("An error occurred in SQL Queery", err);
                return res.status(500).send('Database Error');
            }
            return res.status(200).send("AddOns Updated Successfully");
        })
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).send('Internal Server Error');
    }
}

module.exports = {
    getAddOnsList,
    addAddOnsData,
    removeAddOnsData,
    updateAddOnsData
}