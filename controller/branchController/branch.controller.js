const pool = require('../../database');
const { jsPDF } = require('jspdf');
require('jspdf-autotable');

// Get Branch Dashboard

const getBranchList = (req, res) => {
    try {
        sql_querry_getDetails = `SELECT branchId, branchName FROM branch_data ORDER BY branchName ASC`;
        pool.query(sql_querry_getDetails, (err, data) => {
            if (err) {
                console.error("An error occurd in SQL Queery", err);
                return res.status(500).send('Database Error');
            }
            return res.status(200).send(data);
        })
    } catch (error) {
        console.error('An error occurd', error);
        res.status(500).send('Internal Server Error');
    }
}

// Add Branch API

const addBranch = async (req, res) => {
    try {
        const uid1 = new Date();
        const branchId = String("branchId_" + uid1.getTime());

        const data = {
            branchName: req.body.branchName.trim(),
        }
        if (!data.branchName) {
            return res.status(400).send("Please Fill All The Fields....!");
        } else {
            req.body.productName = pool.query(`SELECT branchName FROM branch_data WHERE branchName = '${data.branchName}'`, function (err, row) {
                if (err) {
                    console.error("An error occurd in SQL Queery", err);
                    return res.status(500).send('Database Error');
                }
                if (row && row.length) {
                    return res.status(400).send('Branch is Already In Use');
                } else {
                    const sql_querry_addCategory = `INSERT INTO branch_data (branchId, branchName)  
                                                    VALUES ('${branchId}','${data.branchName}')`;
                    pool.query(sql_querry_addCategory, (err, data) => {
                        if (err) {
                            console.error("An error occurd in SQL Queery", err);
                            return res.status(500).send('Database Error');
                        }
                        return res.status(200).send("Branch Added Successfully");
                    })
                }
            })
        }
    } catch (error) {
        console.error('An error occurd', error);
        res.status(500).send('Internal Server Error');
    }
}

// Remove Branch API

const removeBranch = async (req, res) => {

    try {
        const branchId = req.query.branchId ? req.query.branchId.trim() : null;
        if (!branchId) {
            return res.status(404).send('Id Not Found');
        }
        req.query.stockInCategoryId = pool.query(`SELECT branchId FROM branch_data WHERE branchId = '${branchId}'`, (err, row) => {
            if (err) {
                console.error("An error occurd in SQL Queery", err);
                return res.status(500).send('Database Error');
            }
            if (row && row.length) {
                const sql_querry_removedetails = `DELETE FROM branch_data WHERE branchId = '${branchId}'`;
                pool.query(sql_querry_removedetails, (err, data) => {
                    if (err) {
                        console.error("An error occurd in SQL Queery", err);
                        return res.status(500).send('Database Error');
                    }
                    return res.status(200).send("Branch Deleted Successfully");
                })
            } else {
                return res.send('BranchId Not Found');
            }
        })
    } catch (error) {
        console.error('An error occurd', error);
        res.status(500).send('Internal Server Error');
    }
}

// Update Branch API

const updateBranch = async (req, res) => {
    try {
        const data = {
            branchId: req.body.branchId.trim(),
            branchName: req.body.branchName.trim()
        }
        if (!data.branchName) {
            res.status(400).send("Please Add branchName");
        }
        const sql_querry_updatedetails = `UPDATE branch_data SET branchName = '${data.branchName}'
                                          WHERE branchId = '${data.branchId}'`;
        pool.query(sql_querry_updatedetails, (err, data) => {
            if (err) {
                console.error("An error occurd in SQL Queery", err);
                return res.status(500).send('Database Error');
            }
            return res.status(200).send("Branch Updated Successfully");
        })
    } catch (error) {
        console.error('An error occurd', error);
        res.status(500).send('Internal Server Error');
    }
}

module.exports = {
    getBranchList,
    addBranch,
    removeBranch,
    updateBranch
}
