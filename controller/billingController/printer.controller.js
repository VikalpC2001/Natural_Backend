const pool = require('../../database');
const jwt = require("jsonwebtoken");


// Get & Add Printer List

const getPrinterList = (req, res) => {
    try {
        const data = {
            macAddress: req.query.macAddress
        }
        if (!data.macAddress) {
            return res.status(404).send('Mac Address Not Found....!');
        } else {
            sql_query_chkMacAddress = `SELECT macAddress FROM billing_setPrinter_data WHERE macAddress = '${data.macAddress}'`;
            pool.query(sql_query_chkMacAddress, (err, row) => {
                if (err) {
                    console.error("An error occurred in SQL Queery", err);
                    return res.status(500).send('Database Error');
                } else {
                    let getPrinterDataByMacAddress = `SELECT
                                                        bsd.printerId AS printerId,
                                                        bsd.categoryId AS categoryId,
                                                        bcpd.categoryName AS categoryName,
                                                        bsd.printerName AS printerName,
                                                        bsd.macAddress AS macAddress,
                                                        bsd.marginTop AS marginTop,
                                                        bsd.marginRight AS marginRight,
                                                        bsd.marginBottom AS marginBottom,
                                                        bsd.marginLeft AS marginLeft
                                                    FROM
                                                        billing_setPrinter_data AS bsd
                                                    INNER JOIN billing_categoryPrinter_data AS bcpd ON bcpd.categoryId = bsd.categoryId
                                                    WHERE macAddress = '${data.macAddress}'`
                    if (row && row.length) {
                        pool.query(getPrinterDataByMacAddress, (err, printer) => {
                            if (err) {
                                console.error("An error occurred in SQL Queery", err);
                                return res.status(500).send('Database Error');
                            } else {
                                return res.status(200).send(printer);
                            }
                        })
                    } else {
                        sql_querry_addPrinterCategory = `INSERT INTO billing_setPrinter_data(printerId, categoryId, printerName, macAddress, marginTop, marginRight, marginBottom, marginLeft)
                                                         SELECT CONCAT('Printer_',LEFT(UUID(), 8)), categoryId, NULL, '${data.macAddress}',0, 0, 0, 0  FROM billing_categoryPrinter_data`;
                        pool.query(sql_querry_addPrinterCategory, (err, category) => {
                            if (err) {
                                console.error("An error occurred in SQL Queery", err);
                                return res.status(500).send('Database Error');
                            } else {
                                pool.query(getPrinterDataByMacAddress, (err, printer) => {
                                    if (err) {
                                        console.error("An error occurred in SQL Queery", err);
                                        return res.status(500).send('Database Error');
                                    } else {
                                        return res.status(200).send(printer);
                                    }
                                })
                            }
                        })
                    }
                }
            })
        }
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).send('Internal Server Error');
    }
}

// Update Printer List

const updatePrinterData = async (req, res) => {
    try {
        const data = {
            categoryId: req.body.categoryId,
            printerName: req.body.printerName,
            macAddress: req.body.macAddress,
            marginTop: req.body.marginTop,
            marginRight: req.body.marginRight,
            marginBottom: req.body.marginBottom,
            marginLeft: req.body.marginLeft
        }
        if (!data.categoryId || !data.macAddress || !data.printerName) {
            return res.status(400).send("Please Fill All The Fields..!");
        } else {
            const sql_querry_updatedetails = `UPDATE
                                                  billing_setPrinter_data
                                              SET
                                                  printerName = '${data.printerName}',
                                                  marginTop = ${data.marginTop},
                                                  marginRight = ${data.marginRight},
                                                  marginBottom = ${data.marginBottom},
                                                  marginLeft = ${data.marginLeft}
                                              WHERE categoryId = '${data.categoryId}' AND macAddress = '${data.macAddress}'`;
            pool.query(sql_querry_updatedetails, (err, data) => {
                if (err) {
                    console.error("An error occurred in SQL Queery", err);
                    return res.status(500).send('Database Error');
                }
                return res.status(200).send("Printer Updated Successfully");
            })
        }
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).send('Internal Server Error');
    }
}

module.exports = {
    updatePrinterData,
    getPrinterList
}

