const pool = require('../../database');
const jwt = require("jsonwebtoken");
const { jsPDF } = require('jspdf');
require('jspdf-autotable');

async function createPDF(res, datas, firmData) {
    try {
        const doc = new jsPDF();
        // Set a large font size for the heading
        const headingText = firmData[0].firmName;
        doc.setFontSize(24);

        // Calculate the width of the text to center it horizontally
        const pageWidth = doc.internal.pageSize.getWidth();
        const textWidth = doc.getTextWidth(headingText);
        const xPosition = (pageWidth - textWidth) / 2; // Calculate the X position to center the text

        // Add heading in the middle of the page
        doc.text(headingText, xPosition, 14);

        // Add address below the heading
        doc.setFontSize(12); // Set font size for the address
        const addressLine = firmData[0].firmAddress;
        const addressXPosition1 = (pageWidth - doc.getTextWidth(addressLine)) / 2;

        doc.text(addressLine, addressXPosition1, 22); // First line of the address

        // Draw a horizontal line after the address
        const lineYPosition = 25; // Y position of the line
        doc.setLineWidth(0.5); // Line width
        doc.line(14, lineYPosition, pageWidth - 14, lineYPosition); // Draw line from left to right

        // Add GST Number below the first line
        doc.setFontSize(12); // Set font size for the GST number
        const gstNumber = `GSTIN : ${firmData[0].gstNumber}`;
        const gstXPosition = (pageWidth - doc.getTextWidth(gstNumber)) / 2;

        doc.text(gstNumber, gstXPosition, lineYPosition + 5.5); // Add GST number 10 units below the first line

        // Draw a second horizontal line below the GST number
        const secondLineYPosition = lineYPosition + 8; // Y position of the second line
        doc.line(14, secondLineYPosition, pageWidth - 14, secondLineYPosition); // Draw line from left to right

        // Add second heading below the second line
        doc.setFontSize(14); // Set a new font size for the second heading
        const secondHeading = 'Periodic Collection Summary';
        const secondHeadingXPosition = (pageWidth - doc.getTextWidth(secondHeading)) / 2;

        doc.text(secondHeading, secondHeadingXPosition, secondLineYPosition + 5.5); // Add second heading 10 units below the second line

        let grandTotals = {
            amount: 0,
            discount: 0,
            cgst: 0,
            sgst: 0,
            total: 0
        };

        function addSection(doc, title, items, isFirstPage = false, footer) {
            if (!isFirstPage) {
                doc.addPage();
            }
            doc.setFontSize(16);
            doc.text(title, 15.1, isFirstPage ? 50 : 20);

            const head = [
                ['Bill No.', 'Amount', 'Discount', 'CGST', 'SGST', 'Total Amount']
            ];

            const tableData = items.map((item) => (
                [item.billNumber,
                parseFloat(item.totalAmount).toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
                parseFloat(item.discountValue).toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
                    0,
                    0,
                parseFloat(item.settledAmount).toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
                ]
            ));
            tableData.push(footer);

            doc.autoTable({
                head: head,
                body: tableData,
                startY: isFirstPage ? 55 : 25,
                theme: 'grid',
                styles: {
                    cellPadding: 2,
                    halign: 'center',
                    fontSize: 10,
                    lineWidth: 0.1, // Add border width
                    lineColor: [192, 192, 192] // Add border color
                },
                headStyles: {
                    lineWidth: 0.1, // Add border width
                    lineColor: [192, 192, 192], // Add border color
                    fontSize: 10,
                    halign: 'center',
                },
                didParseCell: function (data) {
                    // Apply a red background to the entire row if settled amount is 0
                    const settledAmount = parseFloat(data.row.raw[5]);
                    if (settledAmount === 0) {
                        data.cell.styles.fillColor = [255, 0, 79];
                        data.cell.styles.textColor = [255, 255, 255] // Red background for the entire row
                    }
                    var rows = data.table.body;
                    if (data.row.index === rows.length - 1) {
                        data.cell.styles.fontSize = 12;
                        data.cell.styles.fontStyle = 'bold';

                    }
                }
            });

            // Accumulate totals
            grandTotals.amount += parseFloat(footer[1].replace(/,/g, ''));
            grandTotals.discount += parseFloat(footer[2].replace(/,/g, ''));
            grandTotals.total += parseFloat(footer[5].replace(/,/g, ''));
        }

        let isFirstPage = true;
        Object.keys(datas).forEach((key, index) => {
            const section = datas[key];
            const footer = ['Total',
                parseFloat(datas[key].sumOftotalAmount).toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
                parseFloat(datas[key].sumOFtotalDiscount).toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
                '0',
                '0',
                parseFloat(datas[key].sumOfsettledAmount).toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
            ];
            addSection(doc, key, section.items, isFirstPage, footer);
            isFirstPage = false;
        });

        // Add a new page for grand totals
        doc.addPage();
        doc.setFontSize(16);
        doc.text('Grand Total Summary', 14, 20);

        // Add grand total details
        doc.setFontSize(14);
        const grandTotalLines = [
            ['Amount', grandTotals.amount.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })],
            ['Discount', grandTotals.discount.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })],
            ['Total GST (CGST + SGST)', (grandTotals.cgst + grandTotals.sgst).toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })],
            ['Total Amount', grandTotals.total.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })]
        ];

        grandTotalLines.forEach((line, index) => {
            doc.text(line[0], 14, 40 + (index * 10));
            doc.text(line[1] + ' Rs.', pageWidth - 14 - doc.getTextWidth(line[1]), 40 + (index * 10));
        });

        const pdfBytes = await doc.output('arraybuffer');
        const fileName = 'jane-doe.pdf'; // Set the desired file name

        // Set the response headers for the PDF download
        res.setHeader('Content-Disposition', `attachment; filename="${fileName}"`);
        res.setHeader('Content-Type', 'application/pdf');

        // Stream the PDF to the client for download
        res.send(Buffer.from(pdfBytes));

    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).json('Internal Server Error');
    }
}

// Get Firm List

const getFirmData = (req, res) => {
    try {
        const page = req.query.page;
        const numPerPage = req.query.numPerPage;
        const skip = (page - 1) * numPerPage;
        const limit = skip + ',' + numPerPage;
        const searchWord = req.query.searchWord ? req.query.searchWord : '';
        sql_querry_getCountDetails = `SELECT count(*) as numRows FROM billing_firm_data
                                      WHERE firmName LIKE '%` + searchWord + `%'`;
        pool.query(sql_querry_getCountDetails, (err, rows, fields) => {
            if (err) {
                console.error("An error occurred in SQL Queery", err);
                return res.status(500).send('Database Error');
            } else {
                const numRows = rows[0].numRows;
                const numPages = Math.ceil(numRows / numPerPage);
                const sql_query_getDetails = `SELECT
                                                firmId AS firmId,
                                                firmName AS firmName,
                                                gstNumber AS gstNumber,
                                                firmAddress AS firmAddress,
                                                pincode AS pincode,
                                                firmMobileNo AS firmMobileNo,
                                                otherMobileNo AS otherMobileNo
                                              FROM
                                                billing_firm_data AS bfd
                                              WHERE bfd.firmName LIKE '%` + searchWord + `%'
                                              ORDER BY bfd.firmName ASC
                                              LIMIT ${limit}`;
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

// Get Frim Data By ID

const getFirmDataById = (req, res) => {
    try {
        const firmId = req.query.firmId ? req.query.firmId : null;
        if (!firmId) {
            return res.status(404).send("firmId Not Found");
        } else {
            const sql_query_getDetails = `SELECT
                                            firmId AS firmId,
                                            firmName AS firmName,
                                            gstNumber AS gstNumber,
                                            firmAddress AS firmAddress,
                                            pincode AS pincode,
                                            firmMobileNo AS firmMobileNo,
                                            otherMobileNo AS otherMobileNo
                                          FROM
                                            billing_firm_data AS bfd
                                          WHERE bfd.firmId = '${firmId}'`;
            pool.query(sql_query_getDetails, (err, data) => {
                if (err) {
                    console.error("An error occurred in SQL Queery", err);
                    return res.status(500).send('Database Error');;
                } else {
                    return res.status(200).send(data[0]);
                }
            })
        }
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).json('Internal Server Error');
    }
}

// Add Firm API

const addFirmData = async (req, res) => {
    try {
        let token;
        token = req.headers ? req.headers.authorization.split(" ")[1] : null;
        if (token) {
            const decoded = jwt.verify(token, process.env.JWT_SECRET);
            const rights = decoded.id.rights;
            if (rights == 1) {
                const uid1 = new Date();
                const firmId = String("firm_" + uid1.getTime());

                const data = {
                    firmName: req.body.firmName ? req.body.firmName : null,
                    gstNumber: req.body.gstNumber ? req.body.gstNumber : null,
                    firmAddress: req.body.firmAddress ? req.body.firmAddress : null,
                    pincode: req.body.pincode ? req.body.pincode : null,
                    firmMobileNo: req.body.firmMobileNo ? req.body.firmMobileNo : null,
                    otherMobileNo: req.body.otherMobileNo ? req.body.otherMobileNo : null,
                }
                if (!data.firmName || !data.gstNumber || !data.firmAddress || !data.pincode || !data.firmMobileNo) {
                    return res.status(400).send("Please Fill All The Fields...!");
                } else {
                    pool.query(`SELECT firmName FROM billing_firm_data WHERE firmName = '${data.firmName}'`, function (err, row) {
                        if (err) {
                            console.error("An error occurred in SQL Queery", err);
                            return res.status(500).send('Database Error');
                        } else if (row && row.length) {
                            return res.status(400).send('Firm is Already In Use');
                        } else {
                            const sql_querry_addCategory = `INSERT INTO billing_firm_data (firmId, firmName, gstNumber, firmAddress, pincode, firmMobileNo, otherMobileNo)  
                                                            VALUES ('${firmId}','${data.firmName}','${data.gstNumber}','${data.firmAddress}',${data.pincode},'${data.firmMobileNo}',NULLIF('${data.otherMobileNo}','null'))`;
                            pool.query(sql_querry_addCategory, (err, data) => {
                                if (err) {
                                    console.error("An error occurred in SQL Queery", err);
                                    return res.status(500).send('Database Error');
                                }
                                return res.status(200).send("Firm Added Successfully");
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

// Remove Firm API

const removeFirmData = async (req, res) => {
    try {
        let token;
        token = req.headers ? req.headers.authorization.split(" ")[1] : null;
        if (token) {
            const decoded = jwt.verify(token, process.env.JWT_SECRET);
            const rights = decoded.id.rights;
            if (rights == 1) {
                const firmId = req.query.firmId.trim();
                req.query.firmId = pool.query(`SELECT firmId FROM billing_firm_data WHERE firmId = '${firmId}'`, (err, row) => {
                    if (err) {
                        console.error("An error occurred in SQL Queery", err);
                        return res.status(500).send('Database Error');
                    }
                    if (row && row.length) {
                        const sql_querry_removedetails = `DELETE FROM billing_firm_data WHERE firmId = '${firmId}'`;
                        pool.query(sql_querry_removedetails, (err, data) => {
                            if (err) {
                                console.error("An error occurred in SQL Queery", err);
                                return res.status(500).send('Database Error');
                            }
                            return res.status(200).send("Firm Deleted Successfully");
                        })
                    } else {
                        return res.send('FirmId Not Found');
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

// Update Firm API

const updateFirmData = async (req, res) => {
    try {
        const data = {
            firmId: req.body.firmId,
            firmName: req.body.firmName ? req.body.firmName : null,
            gstNumber: req.body.gstNumber ? req.body.gstNumber : null,
            firmAddress: req.body.firmAddress ? req.body.firmAddress : null,
            pincode: req.body.pincode ? req.body.pincode : null,
            firmMobileNo: req.body.firmMobileNo ? req.body.firmMobileNo : null,
            otherMobileNo: req.body.otherMobileNo ? req.body.otherMobileNo : null,
        }
        if (!data.firmId || !data.firmName || !data.gstNumber || !data.firmAddress || !data.pincode || !data.firmMobileNo) {
            return res.status(400).send("Please Fill All The Fields...!");
        } else {

        }
        pool.query(`SELECT firmName FROM billing_firm_data WHERE firmName = '${data.firmName}' AND firmId != '${data.firmId}'`, function (err, row) {
            if (err) {
                console.error("An error occurred in SQL Queery", err);
                return res.status(500).send('Database Error');
            } else if (row && row.length) {
                return res.status(400).send('Firm is Already In Use');
            } else {
                const sql_querry_updatedetails = `UPDATE 
                                                    billing_firm_data 
                                                  SET 
                                                    firmName = '${data.firmName}',
                                                    gstNumber = '${data.gstNumber}',
                                                    firmAddress = '${data.firmAddress}',
                                                    pincode = ${data.pincode},
                                                    firmMobileNo = '${data.firmMobileNo}',
                                                    otherMobileNo = NULLIF('${data.otherMobileNo}','null')
                                                  WHERE firmId = '${data.firmId}'`;
                pool.query(sql_querry_updatedetails, (err, data) => {
                    if (err) {
                        console.error("An error occurred in SQL Queery", err);
                        return res.status(500).send('Database Error');
                    }
                    return res.status(200).send("Firm Updated Successfully");
                })
            }
        })
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).send('Internal Server Error');
    }
}

// DDL Firm Data

const ddlFirmData = (req, res) => {
    try {
        var sql_queries_getDetails = `SELECT
                                        firmId,
                                        firmName
                                      FROM
                                        billing_firm_data`;

        pool.query(sql_queries_getDetails, (err, rows, fields) => {
            if (err) {
                console.error("An error occurred in SQL Queery", err);
                return res.status(500).send('Database Error');;
            } else {
                return res.status(200).send(rows);
            }
        });
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).send('Internal Server Error');
    }
}

// Get Bill Data By Firm Id

const getBillDataByFirmId = (req, res) => {
    try {
        var date = new Date();
        var lastDay = date.toString().slice(4, 15);
        var firstDay = new Date(date);
        firstDay.setMonth(firstDay.getMonth() - 1);
        firstDay = firstDay.toString().slice(4, 15);
        const firmId = req.query.firmId ? req.query.firmId : null;
        const page = req.query.page;
        const numPerPage = req.query.numPerPage;
        const skip = (page - 1) * numPerPage;
        const limit = skip + ',' + numPerPage;
        const billPayType = req.query.billPayType ? req.query.billPayType : null;
        const billType = req.query.billType ? req.query.billType : null;
        const startDate = (req.query.startDate ? req.query.startDate : '').slice(4, 15);
        const endDate = (req.query.endDate ? req.query.endDate : '').slice(4, 15);
        const searchWord = req.query.searchWord ? req.query.searchWord : '';

        if (!firmId) {
            return res.status(404).send('firmId Not Found....!');
        } else {
            let sql_querry_getCountDetails = `SELECT count(*) as numRows FROM billing_Official_data AS bod
                                              WHERE bod.firmId = '${firmId}'
                                              AND billDate BETWEEN STR_TO_DATE('${startDate ? startDate : firstDay}','%b %d %Y') AND STR_TO_DATE('${endDate ? endDate : lastDay}','%b %d %Y')
                                              ${billType ? `AND bod.billType = '${billType}'` : ''}
                                              ${billPayType ? `AND bod.billPayType = '${billPayType}'` : ''}`;
            pool.query(sql_querry_getCountDetails, (err, rows, fields) => {
                if (err) {
                    console.error("An error occurred in SQL Queery", err);
                    return res.status(500).send('Database Error');
                } else {
                    const numRows = rows[0].numRows;
                    const numPages = Math.ceil(numRows / numPerPage);
                    const sql_query_getDetails = `SELECT
                                                    bod.billId AS billId,
                                                    bod.billNumber AS billNumber,
                                                    bod.firmId AS firmId,
                                                    bod.cashier AS cashier,
                                                    bod.billType AS billType,
                                                    bod.billPayType AS billPayType,
                                                    bod.totalAmount AS totalAmount,
                                                    bod.settledAmount AS settledAmount,
                                                    DATE_FORMAT(bod.billDate,"%d-%m-%Y") AS billDate,
                                                    DATE_FORMAT(bod.billCreationDate,"%h:%i %p") AS billCreationDate
                                                  FROM
                                                    billing_Official_data AS bod
                                                  WHERE firmId = '${firmId}'
                                                  AND billDate BETWEEN STR_TO_DATE('${startDate ? startDate : firstDay}','%b %d %Y') AND STR_TO_DATE('${endDate ? endDate : lastDay}','%b %d %Y') 
                                                  ${billType ? `AND bod.billType = '${billType}'` : ''}
                                                  ${billPayType ? `AND bod.billPayType = '${billPayType}'` : ''}
                                                  ORDER BY bod.billCreationDate DESC, bod.billNumber DESC
                                                  LIMIT ${limit}`;
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
            });
        }
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).send('Internal Server Error');
    }
}

// Get Bill Data By Firm Id

const getComplimentaryBillDataByFirmId = (req, res) => {
    try {
        var date = new Date();
        var lastDay = date.toString().slice(4, 15);
        var firstDay = new Date(date);
        firstDay.setMonth(firstDay.getMonth() - 1);
        firstDay = firstDay.toString().slice(4, 15);
        const firmId = req.query.firmId ? req.query.firmId : null;
        const page = req.query.page;
        const numPerPage = req.query.numPerPage;
        const skip = (page - 1) * numPerPage;
        const limit = skip + ',' + numPerPage;
        const billPayType = req.query.billPayType ? req.query.billPayType : null;
        const billType = req.query.billType ? req.query.billType : null;
        const startDate = (req.query.startDate ? req.query.startDate : '').slice(4, 15);
        const endDate = (req.query.endDate ? req.query.endDate : '').slice(4, 15);
        const searchWord = req.query.searchWord ? req.query.searchWord : '';

        if (!firmId) {
            return res.status(404).send('firmId Not Found....!');
        } else {
            let sql_querry_getCountDetails = `SELECT count(*) as numRows FROM billing_Complimentary_data AS bcd
                                              WHERE bcd.firmId = '${firmId}'
                                              AND bcd.billDate BETWEEN STR_TO_DATE('${startDate ? startDate : firstDay}','%b %d %Y') AND STR_TO_DATE('${endDate ? endDate : lastDay}','%b %d %Y')
                                              ${billType ? `AND bcd.billType = '${billType}'` : ''}
                                              ${billPayType ? `AND bcd.billPayType = '${billPayType}'` : ''}`;
            pool.query(sql_querry_getCountDetails, (err, rows, fields) => {
                if (err) {
                    console.error("An error occurred in SQL Queery", err);
                    return res.status(500).send('Database Error');
                } else {
                    const numRows = rows[0].numRows;
                    const numPages = Math.ceil(numRows / numPerPage);
                    const sql_query_getDetails = `SELECT
                                                    bcd.billId AS billId,
                                                    bcd.billNumber AS billNumber,
                                                    bcd.firmId AS firmId,
                                                    bcd.cashier AS cashier,
                                                    bcd.billType AS billType,
                                                    bcd.billPayType AS billPayType,
                                                    bcd.totalAmount AS totalAmount,
                                                    bcd.settledAmount AS settledAmount,
                                                    DATE_FORMAT(bcd.billDate,"%d-%m-%Y") AS billDate,
                                                    DATE_FORMAT(bcd.billCreationDate,"%h:%i %p") AS billCreationDate
                                                  FROM
                                                    billing_Complimentary_data AS bcd
                                                  WHERE firmId = '${firmId}'
                                                  AND billDate BETWEEN STR_TO_DATE('${startDate ? startDate : firstDay}','%b %d %Y') AND STR_TO_DATE('${endDate ? endDate : lastDay}','%b %d %Y') 
                                                  ${billType ? `AND bcd.billType = '${billType}'` : ''}
                                                  ${billPayType ? `AND bcd.billPayType = '${billPayType}'` : ''}
                                                  ORDER BY bcd.billCreationDate DESC, bcd.billNumber DESC
                                                  LIMIT ${limit}`;
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
            });
        }
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).send('Internal Server Error');
    }
}

// Get Cancel Bill Data By Firm Id

const getCancelBillDataByFirmId = (req, res) => {
    try {
        var date = new Date();
        var lastDay = date.toString().slice(4, 15);
        var firstDay = new Date(date);
        firstDay.setMonth(firstDay.getMonth() - 1);
        firstDay = firstDay.toString().slice(4, 15);
        const firmId = req.query.firmId ? req.query.firmId : null;
        const page = req.query.page;
        const numPerPage = req.query.numPerPage;
        const skip = (page - 1) * numPerPage;
        const limit = skip + ',' + numPerPage;
        const billType = req.query.billType ? req.query.billType : null;
        const startDate = (req.query.startDate ? req.query.startDate : '').slice(4, 15);
        const endDate = (req.query.endDate ? req.query.endDate : '').slice(4, 15);
        const searchWord = req.query.searchWord ? req.query.searchWord : '';

        if (!firmId) {
            return res.status(404).send('firmId Not Found....!');
        } else {
            let sql_querry_getCountDetails = `SELECT count(*) as numRows FROM billing_Official_data AS bod
                                              WHERE bod.firmId = '${firmId}' AND (bod.billPayType = 'cancel' OR bod.billStatus = 'cancel')
                                              AND billDate BETWEEN STR_TO_DATE('${startDate ? startDate : firstDay}','%b %d %Y') AND STR_TO_DATE('${endDate ? endDate : lastDay}','%b %d %Y')
                                              ${billType ? `AND bod.billType = '${billType}'` : ''}`;
            pool.query(sql_querry_getCountDetails, (err, rows, fields) => {
                if (err) {
                    console.error("An error occurred in SQL Queery", err);
                    return res.status(500).send('Database Error');
                } else {
                    const numRows = rows[0].numRows;
                    const numPages = Math.ceil(numRows / numPerPage);
                    const sql_query_getDetails = `SELECT
                                                    bod.billId AS billId,
                                                    bod.billNumber AS billNumber,
                                                    bod.firmId AS firmId,
                                                    bod.cashier AS cashier,
                                                    bod.billType AS billType,
                                                    bod.billPayType AS billPayType,
                                                    bod.totalAmount AS totalAmount,
                                                    bod.settledAmount AS settledAmount,
                                                    DATE_FORMAT(bod.billDate,"%d-%m-%Y") AS billDate,
                                                    DATE_FORMAT(bod.billCreationDate,"%h:%i %p") AS billCreationDate
                                                  FROM
                                                    billing_Official_data AS bod
                                                  WHERE firmId = '${firmId}' AND (bod.billPayType = 'cancel' OR bod.billStatus = 'cancel')
                                                  AND billDate BETWEEN STR_TO_DATE('${startDate ? startDate : firstDay}','%b %d %Y') AND STR_TO_DATE('${endDate ? endDate : lastDay}','%b %d %Y') 
                                                  ${billType ? `AND bod.billType = '${billType}'` : ''}
                                                  ORDER BY bod.billCreationDate DESC, bod.billNumber DESC
                                                  LIMIT ${limit}`;
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
            });
        }
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).send('Internal Server Error');
    }
}

// Get Month Wise Bill Data By Firm

const getMonthWiseBillDataByFirmId = (req, res) => {
    try {
        const firmId = req.query.firmId ? req.query.firmId : null;
        const page = req.query.page;
        const numPerPage = req.query.numPerPage;
        const skip = (page - 1) * numPerPage;
        const limit = skip + ',' + numPerPage;

        if (!firmId) {
            return res.status(404).send('firmId Not Found....!');
        } else {
            let sql_querry_getCountDetails = `SELECT COUNT(*) AS numRows FROM (
                                                  SELECT YEAR(billDate), MONTH(billDate) FROM billing_Official_data
                                                  WHERE firmId = '${firmId}'
                                                  AND LOWER(billStatus) != 'cancel'
                                                  AND LOWER(billPayType) NOT IN ('complimentary', 'cancel')
                                                  GROUP BY
                                                      YEAR(billDate),
                                                      MONTH(billDate)
                                              ) AS monthlySummary`;
            pool.query(sql_querry_getCountDetails, (err, rows, fields) => {
                if (err) {
                    console.error("An error occurred in SQL Queery", err);
                    return res.status(500).send('Database Error');
                } else {
                    const numRows = rows[0].numRows;
                    const numPages = Math.ceil(numRows / numPerPage);
                    const sql_query_getDetails = `SELECT
                                                      DATE_FORMAT(billDate, '%b - %Y') AS monthYear,
                                                      DATE_FORMAT(DATE_FORMAT(billDate, '%Y-%m-01'), '%b %d %Y') AS firstDate,
                                                      DATE_FORMAT(LAST_DAY(billDate), '%b %d %Y') AS lastDate,
                                                      SUM(totalAmount) AS totalBillAmount,
                                                      SUM(settledAmount) AS totalSettledAmount
                                                  FROM
                                                      billing_Official_data
                                                  WHERE
                                                      firmId = '${firmId}'
                                                      AND LOWER(billStatus) != 'cancel'
                                                      AND LOWER(billPayType) NOT IN ('complimentary', 'cancel')
                                                  GROUP BY
                                                      YEAR(billDate),
                                                      MONTH(billDate)
                                                  ORDER BY
                                                      YEAR(billDate) DESC,
                                                      MONTH(billDate) DESC
                                                  LIMIT ${limit}`;
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
            });
        }
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).send('Internal Server Error');
    }
}

// Get Statics Data By Firm

const getStaticsDataByFirmId = (req, res) => {
    try {
        const firmId = req.query.firmId ? req.query.firmId : null;
        var date = new Date(), y = date.getFullYear(), m = (date.getMonth());
        var firstDay = new Date(y, m, 1).toString().slice(4, 15);
        var lastDay = new Date(y, m + 1, 0).toString().slice(4, 15);
        const startDate = (req.query.startDate ? req.query.startDate : '').slice(4, 15);
        const endDate = (req.query.endDate ? req.query.endDate : '').slice(4, 15);

        if (!firmId) {
            return res.status(404).send('firmId Not Found....!');
        } else {
            let sql_query_getStatics = `SELECT 
                                            SUM(settledAmount) AS totalBusiness,                                   
                                            SUM(CASE WHEN billPayType = 'Cash' AND billStatus != 'Cancel' THEN settledAmount ELSE 0 END) AS totalCashBusiness,
                                            SUM(CASE WHEN billPayType = 'Debit' AND billStatus != 'Cancel' THEN settledAmount ELSE 0 END) AS totalDebitBusiness,
                                            SUM(CASE WHEN billPayType = 'Due' AND billStatus != 'Cancel' THEN settledAmount ELSE 0 END) AS totalDueBusiness,
                                            SUM(CASE WHEN billPayType = 'Online' AND billStatus != 'Cancel' THEN settledAmount ELSE 0 END) AS totalOnlineBusiness,
                                            SUM(CASE WHEN billPayType = 'Cancel' OR billStatus = 'Cancel' THEN settledAmount ELSE 0 END) AS totalCancel                    
                                        FROM 
                                            billing_Official_data
                                        WHERE firmId = '${firmId}'
                                        AND billDate BETWEEN STR_TO_DATE('${startDate ? startDate : firstDay}','%b %d %Y') AND STR_TO_DATE('${endDate ? endDate : lastDay}','%b %d %Y')`;
            pool.query(sql_query_getStatics, (err, data) => {
                if (err) {
                    console.error("An error occurred in SQL Queery", err);
                    return res.status(500).send('Database Error');;
                } else {
                    return res.status(200).send(data[0]);
                }
            })
        }
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).send('Internal Server Error');
    }
}

// Export Tax Report By Firm Id

const getTaxReportByFirmId = (req, res) => {
    try {
        var date = new Date(), y = date.getFullYear(), m = (date.getMonth());
        var firstDay = new Date(y, m, 1).toString().slice(4, 15);
        var lastDay = new Date(y, m + 1, 0).toString().slice(4, 15);

        const data = {
            firmId: req.query.firmId,
            billPayType: req.query.billPayType,
            startDate: (req.query.startDate ? req.query.startDate : ''),
            endDate: (req.query.endDate ? req.query.endDate : '')
        }
        if (!data.firmId) {
            return res.status(404).send('Firm Not Found');
        }
        const sql_query_staticQuery = `SELECT
                                            CASE
                                                WHEN billType = 'Hotel' AND billPayType = 'cash' THEN CONCAT('HC/ ',billNumber)
                                                WHEN billType = 'Hotel' AND billPayType = 'debit' THEN CONCAT('HD/ ',billNumber)
                                                ELSE billNumber
                                            END AS billNumber,
                                            billType,
                                            billPayType,
                                            discountType,
                                            discountValue,
                                            totalDiscount,
                                            totalAmount,
                                            CASE
                                                WHEN billStatus = 'Cancel' THEN 0
                                                ELSE settledAmount
                                            END AS settledAmount,
                                            DATE_FORMAT(billDate,'%b %d %Y, %W') AS billDate,
                                            billStatus
                                       FROM
                                            billing_Official_data`;
        const sql_query_getFirmData = `SELECT
                                           firmName,
                                           gstNumber,
                                           CONCAT(firmAddress,' - ',pincode) AS firmAddress
                                       FROM
                                           billing_firm_data
                                       WHERE
                                           firmId = '${data.firmId}'`
        let sql_querry_getDetails = `${sql_query_staticQuery}
                                     WHERE firmId = '${data.firmId}' 
                                     ${data.billPayType ? `AND billPayType = '${data.billPayType}'` : ''}
                                     AND billDate BETWEEN STR_TO_DATE('${data.startDate ? data.startDate : firstDay}','%b %d %Y') AND STR_TO_DATE('${data.endDate ? data.endDate : lastDay}','%b %d %Y') 
                                     ORDER BY billing_Official_data.billDate ASC, billing_Official_data.billNumber ASC;
                                     ${sql_query_getFirmData}`;
        pool.query(sql_querry_getDetails, (err, data) => {
            if (err) {
                console.error("An error occurred in SQL Queery", err);
                return res.status(500).send('Database Error');
            } else {
                if (data && data[0].length) {
                    const result = data[0].reduce((acc, item) => {
                        const key = item.billDate;
                        if (!acc[key]) {
                            acc[key] = {
                                items: [],
                                sumOFtotalDiscount: 0,
                                sumOftotalAmount: 0,
                                sumOfsettledAmount: 0
                            };
                        }
                        acc[key].items.push(item);
                        if (item.billStatus != 'Cancel') {
                            acc[key].sumOFtotalDiscount += item.totalDiscount;
                            acc[key].sumOftotalAmount += item.totalAmount;
                            acc[key].sumOfsettledAmount += item.settledAmount;
                        }
                        return acc;
                    }, {});
                    const firmdata = data && data[1].length ? data[1] : null;
                    createPDF(res, result, firmdata)
                        .then(() => {
                            console.log('PDF created successfully');
                            res.status(200);
                        })
                        .catch((err) => {
                            console.log(err);
                            res.status(500).send('Error creating PDF');
                        });
                } else {
                    return res.status(404).send('No Data Found');
                }
            }
        })
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).send('Internal Server Error');
    }
}

module.exports = {
    getFirmData,
    getFirmDataById,
    addFirmData,
    removeFirmData,
    updateFirmData,
    ddlFirmData,
    getTaxReportByFirmId,
    getBillDataByFirmId,
    getCancelBillDataByFirmId,
    getComplimentaryBillDataByFirmId,
    getMonthWiseBillDataByFirmId,
    getStaticsDataByFirmId
}