const pool = require('../../database');
const pool2 = require('../../databasePool');

function getAddressValidation(addressDetails) {

    const addressCount = addressDetails.reduce((acc, curr) => {
        const address = curr.address.toLowerCase(); // Normalize address to lowercase
        acc[address] = (acc[address] || 0) + 1; // Increment count
        return acc;
    }, {});

    const isAddressRepeated = Object.values(addressCount).some(count => count > 1);

    return isAddressRepeated;
}

// Get Statics Data By Customer Data

const getStaticsByCustomer = (req, res) => {
    try {
        const customerId = req.query.customerId || null;
        const startDate = req.query.startDate ? req.query.startDate.slice(4, 15) : null;
        const endDate = req.query.endDate ? req.query.endDate.slice(4, 15) : null;

        if (!customerId) {
            return res.status(400).send('customerId Not Found');
        }
        const sql = `SELECT
                         -- ===============================
                         -- ORDER TYPE WISE (NON-Cancel)
                         -- ===============================
                         COALESCE(SUM(CASE WHEN bd.billType = 'Pick Up' AND bd.billStatus <> 'Cancel' THEN 1 ELSE 0 END), 0) AS totalPickupOrders,
                         COALESCE(SUM(CASE WHEN bd.billType = 'Pick Up' AND bd.billStatus <> 'Cancel' THEN bd.settledAmount ELSE 0 END), 0) AS totalPickupPayment,
                         
                         COALESCE(SUM(CASE WHEN bd.billType = 'Delivery' AND bd.billStatus <> 'Cancel' THEN 1 ELSE 0 END), 0) AS totalDeliveryOrders,
                         COALESCE(SUM(CASE WHEN bd.billType = 'Delivery' AND bd.billStatus <> 'Cancel' THEN bd.settledAmount ELSE 0 END), 0) AS totalDeliveryPayment,
                         
                         COALESCE(SUM(CASE WHEN bd.billType = 'Dine In' AND bd.billStatus <> 'Cancel' THEN 1 ELSE 0 END), 0) AS totalDineInOrders,
                         COALESCE(SUM(CASE WHEN bd.billType = 'Dine In' AND bd.billStatus <> 'Cancel' THEN bd.settledAmount ELSE 0 END), 0) AS totalDineInPayment,
                         
                         -- ===============================
                         -- PAYMENT TYPE WISE
                         -- ===============================
                         COALESCE(SUM(CASE WHEN bd.billPayType = 'cash' AND bd.billStatus <> 'Cancel' THEN 1 ELSE 0 END), 0) AS totalCashOrders,
                         COALESCE(SUM(CASE WHEN bd.billPayType = 'cash' AND bd.billStatus <> 'Cancel' THEN bd.settledAmount ELSE 0 END), 0) AS totalCashPayment,
                         
                         COALESCE(SUM(CASE WHEN bd.billPayType = 'due' AND bd.billStatus <> 'Cancel' THEN 1 ELSE 0 END), 0) AS totalDueOrders,
                         COALESCE(SUM(CASE WHEN bd.billPayType = 'due' AND bd.billStatus <> 'Cancel' THEN bd.settledAmount ELSE 0 END), 0) AS totalDuePayment,
                         
                         COALESCE(SUM(CASE WHEN bd.billPayType = 'online' AND bd.billStatus <> 'Cancel' THEN 1 ELSE 0 END), 0) AS totalOnlineOrders,
                         COALESCE(SUM(CASE WHEN bd.billPayType = 'online' AND bd.billStatus <> 'Cancel' THEN bd.settledAmount ELSE 0 END), 0) AS totalOnlinePayment,
                         
                         COALESCE(SUM(CASE WHEN bd.billPayType = 'complimentary' AND bd.billStatus <> 'Cancel' THEN 1 ELSE 0 END), 0) AS totalComplimentaryOrders,
                         COALESCE(SUM(CASE WHEN bd.billPayType = 'complimentary' AND bd.billStatus <> 'Cancel' THEN bd.settledAmount ELSE 0 END), 0) AS totalComplimentaryPayment,
                         
                         -- ===============================
                         -- Cancel (OVERALL)
                         -- ===============================
                         COALESCE(SUM(CASE WHEN bd.billStatus = 'Cancel' THEN 1 ELSE 0 END), 0) AS totalCancelOrders,
                         COALESCE(SUM(CASE WHEN bd.billStatus = 'Cancel' THEN bd.settledAmount ELSE 0 END), 0) AS totalCancelAmount,
                         
                         -- ===============================
                         -- DISCOUNT
                         -- ===============================
                         COALESCE(SUM(CASE WHEN bd.billStatus <> 'Cancel' THEN bd.totalDiscount ELSE 0 END), 0) AS totalDiscount,
                         
                         -- ===============================
                         -- VISITS
                         -- ===============================
                         COALESCE(COUNT(DISTINCT CASE WHEN bd.billStatus <> 'Cancel' THEN bd.billId END), 0) AS totalVisit,
                         
                         COALESCE(
                             DATE_FORMAT(
                                 MAX(CASE WHEN bd.billStatus <> 'Cancel' THEN bd.billCreationDate END),
                                 '%d %b %Y'
                             ),
                             'No Visited'
                         ) AS lastVisitDate,

                         COALESCE(
                             DATE_FORMAT(
                                 MAX(CASE WHEN bd.billStatus <> 'Cancel' THEN bd.billCreationDate END),
                                 '%r'
                             ),
                             'No Visited'
                         ) AS lastVisitTime,
                         
                         -- ===============================
                         -- AVERAGES
                         -- ===============================
                         COALESCE(
                             COUNT(
                                 DISTINCT CASE
                                     WHEN bd.billStatus <> 'Cancel'
                                      AND bd.billDate >= DATE_FORMAT(CURDATE() - INTERVAL 1 MONTH, '%Y-%m-01')
                                      AND bd.billDate <  DATE_FORMAT(CURDATE(), '%Y-%m-01')
                                     THEN bd.billId
                                 END
                             ),
                             0
                         ) AS lastMonthVisit,
                          
                         COALESCE(
                             SUM(CASE WHEN bd.billStatus <> 'Cancel' THEN bd.settledAmount ELSE 0 END),
                             0
                         ) AS totalBusiness
                     FROM billing_data bd
                     JOIN billing_billwisecustomer_data bwc ON bd.billId = bwc.billId
                         
                     WHERE bwc.customerId = ?
                       AND (
                             ( ? IS NOT NULL AND ? IS NOT NULL
                               AND DATE(bd.billDate) BETWEEN 
                                   STR_TO_DATE(?, '%b %d %Y')
                                   AND STR_TO_DATE(?, '%b %d %Y')
                             )
                             OR
                             ( ? IS NULL AND ? IS NULL
                               AND YEAR(bd.billDate) = YEAR(CURDATE())
                             )
                           );`
        const params = [
            customerId,
            startDate, endDate, startDate, endDate, // BETWEEN
            startDate, endDate                      // NULL condition
        ];

        pool.query(sql, params, (err, data) => {
            if (err) {
                console.error('SQL Error:', err);
                return res.status(500).send('Database Error');
            }
            const json = {
                "Pickup Summary": data[0].totalPickupOrders + ' | ₹ ' + parseFloat(data[0].totalPickupPayment ? data[0].totalPickupPayment : 0).toLocaleString('en-IN'),
                "DineIn Summary": data[0].totalDineInOrders + ' | ₹ ' + parseFloat(data[0].totalDineInPayment ? data[0].totalDineInPayment : 0).toLocaleString('en-IN'),

                "Cash Payment": data[0].totalCashOrders + ' | ₹ ' + parseFloat(data[0].totalCashPayment ? data[0].totalCashPayment : 0).toLocaleString('en-IN'),
                "Due Payment": data[0].totalDueOrders + ' | ₹ ' + parseFloat(data[0].totalDuePayment ? data[0].totalDuePayment : 0).toLocaleString('en-IN'),
                "Online Payment": data[0].totalOnlineOrders + ' | ₹ ' + parseFloat(data[0].totalOnlinePayment ? data[0].totalOnlinePayment : 0).toLocaleString('en-IN'),
                "Complimentary Order": data[0].totalComplimentaryOrders + ' | ₹ ' + parseFloat(data[0].totalComplimentaryPayment ? data[0].totalComplimentaryPayment : 0).toLocaleString('en-IN'),

                "Cancel Order": data[0].totalCancelOrders + ' | ₹ ' + parseFloat(data[0].totalCancelAmount ? data[0].totalCancelAmount : 0).toLocaleString('en-IN'),

                "Total Discount": '₹ ' + parseFloat(data[0].totalDiscount ? data[0].totalDiscount : 0).toLocaleString('en-IN'),
                "Visit": data[0].totalVisit,
                "Last Visited": data[0].lastVisitDate,
                "Last Visited Time": data[0].lastVisitTime,
                "Total Business": '₹ ' + parseFloat(data[0].totalBusiness ? data[0].totalBusiness : 0).toLocaleString('en-IN'),
                "Total Last Month Visit": data[0].lastMonthVisit
            }
            return res.status(200).json(json);
        });

    } catch (error) {
        console.error('Server Error:', error);
        res.status(500).send('Internal Server Error');
    }
};

// Get Bill Data By Customer Id

const getBillDataBycustomerId = (req, res) => {
    try {
        const customerId = req.query.customerId || null;
        const page = Number(req.query.page || 1);
        const numPerPage = Number(req.query.numPerPage || 10);
        const skip = (page - 1) * numPerPage;

        const searchWord = req.query.searchWord || '';
        const startDate = req.query.startDate ? req.query.startDate.slice(4, 15) : null;
        const endDate = req.query.endDate ? req.query.endDate.slice(4, 15) : null;

        if (!customerId) {
            return res.status(400).send('customerId Not Found!');
        }

        /* ================= COUNT QUERY ================= */
        let countSql = `SELECT COUNT(*) AS numRows FROM billing_data bd
                        JOIN billing_billwisecustomer_data bwc ON bd.billId = bwc.billId
                        WHERE bwc.customerId = ?
                          AND (
                                (? IS NOT NULL AND ? IS NOT NULL
                                    AND DATE(bd.billDate) BETWEEN
                                        STR_TO_DATE(?, '%b %d %Y')
                                        AND STR_TO_DATE(?, '%b %d %Y')
                                )
                                OR
                                (? IS NULL AND ? IS NULL
                                    AND YEAR(bd.billDate) = YEAR(CURDATE())
                                )
                              )`;

        let countParams = [
            customerId,
            startDate, endDate,
            startDate, endDate,
            startDate, endDate
        ];

        pool.query(countSql, countParams, (err, countResult) => {
            if (err) {
                console.error('Count Query Error:', err);
                return res.status(500).send('Database Error');
            }

            const numRows = countResult[0].numRows;
            console.log(numRows)
            if (numRows === 0) {
                return res.status(200).send({ rows: [], numRows: 0 });
            }

            /* ================= DATA QUERY ================= */
            let dataSql = `SELECT
                                bd.billId AS billId,
                                CASE
                                    WHEN bd.billType = 'Hotel' THEN CONCAT('H',btd.tokenNo)
                                    WHEN bd.billType = 'Pick Up' THEN CONCAT('P',btd.tokenNo)
                                    WHEN bd.billType = 'Delivery' THEN CONCAT('D',btd.tokenNo)
                                    WHEN bd.billType = 'Dine In' THEN CONCAT('R',btd.tokenNo)
                                    ELSE NULL
                                END AS tokenNo,
                                bd.firmId AS firmId,
                                bd.cashier AS cashier,
                                bd.billType AS billType,
                                bd.billPayType AS billPayType,
                                bd.totalAmount AS totalAmount,
                                bd.totalDiscount AS totalDiscount,
                                bd.settledAmount AS settledAmount,
                                DATE_FORMAT(bd.billDate, '%d-%m-%Y') AS billDate,
                                DATE_FORMAT(bd.billCreationDate, '%h:%i %p') AS billCreationDate
                            FROM billing_data bd
                            LEFT JOIN billing_token_data AS btd ON btd.billId = bd.billId
                            LEFT JOIN billing_billwisecustomer_data bwc ON bd.billId = bwc.billId
                            WHERE bwc.customerId = ?
                              AND (
                                    (? IS NOT NULL AND ? IS NOT NULL
                                        AND DATE(bd.billDate) BETWEEN
                                            STR_TO_DATE(?, '%b %d %Y')
                                            AND STR_TO_DATE(?, '%b %d %Y')
                                    )
                                    OR
                                    (? IS NULL AND ? IS NULL
                                        AND YEAR(bd.billDate) = YEAR(CURDATE())
                                    )
                                  )
                            ORDER BY bd.billCreationDate DESC, bd.billNumber DESC
                            LIMIT ?, ?`;

            let dataParams = [
                customerId,
                startDate, endDate,
                startDate, endDate,
                startDate, endDate
            ];

            dataParams.push(skip, numPerPage);

            pool.query(dataSql, dataParams, (err, rows) => {
                if (err) {
                    console.error('Data Query Error:', err);
                    return res.status(500).send('Database Error');
                }

                return res.status(200).send({
                    rows,
                    numRows
                });
            });
        });

    } catch (error) {
        console.error('API Error:', error);
        res.status(500).send('Internal Server Error');
    }
};

// Search Customer Data

const searchCustomerData = (req, res) => {
    try {
        const searchWord = req.query.searchWord;
        var sql_queries_searchCustomer = `SELECT
                                              bcd.customerId AS customerId,
                                              bcd.customerName AS customerName,
                                              bcd.customerMobileNumber AS mobileNo,
                                              bcad.addressId AS addressId,
                                              bcad.customerAddress AS address,
                                              bcad.customerLocality AS locality
                                          FROM
                                              billing_customer_data AS bcd
                                          LEFT JOIN billing_customerAddress_data AS bcad ON bcad.customerId = bcd.customerId
                                          WHERE bcd.customerMobileNumber LIKE '%` + searchWord + `%'`;
        pool.query(sql_queries_searchCustomer, (err, rows, fields) => {
            if (err) {
                console.error("An error occurred in SQL Queery", err);
                return res.status(500).send('Database Error');;
            } else {
                if (searchWord) {
                    return res.status(200).send(rows);
                } else {
                    return res.status(200).send([]);
                }
            }
        });
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).json('Internal Server Error');
    }
};

// Get Customer Data

const getCustomerList = (req, res) => {
    try {
        const page = req.query.page;
        const numPerPage = req.query.numPerPage;
        const skip = (page - 1) * numPerPage;
        const limit = skip + ',' + numPerPage;
        const searchWord = req.query.searchWord ? req.query.searchWord : '';
        sql_querry_getCountDetails = `SELECT count(*) as numRows FROM billing_customer_data AS bcd
                                      WHERE bcd.customerName LIKE '%` + searchWord + `%'
                                      OR bcd.customerMobileNumber LIKE '%` + searchWord + `%'`;
        pool.query(sql_querry_getCountDetails, (err, rows, fields) => {
            if (err) {
                console.error("An error occurred in SQL Queery", err);
                return res.status(500).send('Database Error');
            } else {
                const numRows = rows[0].numRows;
                const numPages = Math.ceil(numRows / numPerPage);
                const sql_query_getDetails = `SELECT 
                                                bcd.customerId,
                                                bcd.customerName,
                                                bcd.customerMobileNumber,
                                                DATE_FORMAT(bcd.birthDate,'%b %d %Y') AS birthDate,
                                                DATE_FORMAT(bcd.anniversaryDate,'%b %d %Y') AS anniversaryDate
                                              FROM 
                                                billing_customer_data AS bcd
                                              WHERE bcd.customerName LIKE '%` + searchWord + `%'
                                              OR bcd.customerMobileNumber LIKE '%` + searchWord + `%'
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

// Get Customer Details By Id

const getCustomerDetailsById = (req, res) => {
    try {
        const customerId = req.query.customerId ? req.query.customerId : null;
        if (!customerId) {
            return res.status(404).send("customerId Not Found");
        } else {
            let sql_query_getCustomerData = `SELECT
                                             customerId AS customerId,
                                             customerName AS customerName,
                                             customerMobileNumber AS mobileNumber,
                                             birthDate AS birthDate,
                                             anniversaryDate AS anniversaryDate
                                         FROM
                                             billing_customer_data
                                         WHERE customerId = '${customerId}';
                                         SELECT
                                             addressId AS addressId,
                                             customerAddress AS address,
                                             customerLocality AS locality
                                         FROM
                                             billing_customerAddress_data
                                         WHERE customerId = '${customerId}'`;
            pool.query(sql_query_getCustomerData, (err, data) => {
                if (err) {
                    console.error("An error occurred in SQL Queery", err);
                    return res.status(500).send('Database Error');;
                } else {
                    if (data && data[0].length) {
                        let json = {
                            ...data[0][0],
                            addressDetails: data[1]
                        }
                        return res.status(200).send(json);
                    } else {
                        return res.status(404).send('No Data Found');
                    }
                }
            })
        }
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).json('Internal Server Error');
    }
}

// Add Multiple Customer Data API

const addMultipleCustomerData = (req, res) => {
    try {
        const uid1 = new Date();
        const newCustometId = String("customer_" + uid1.getTime());
        const newAddressId = String("addressId_" + uid1.getTime());
        const data = {
            mobileNo: req.bdy.mobileNo ? req.body.mobileNo : null,
            name: req.body.name ? req.body.name : null,
            address: req.body.address ? req.body.address : null,
            locality: req.body.locality ? req.body.locality : null
        }
        if (!data.mobileNo) {
            return res.status(404).send('Mobile Number Not Found..!');
        }
        let chk_sql_mobileNoExist = `SELECT customerId FROM billing_customer_data WHERE customerMobileNumber = '${data.mobileNo}'`;
        pool.query(chk_sql_mobileNoExist, (err, no) => {
            if (err) {
                console.error("An error occurred in SQL Queery", err);
                return res.status(500).send('Database Error');
            } else {
                const existCustomerId = no && no.length ? no[0].customerId : null;
                if (existCustomerId) {
                    if (data.address) {
                        sql_query_chkAddress = `SELECT customerAddress FROM billing_customerAddress_data WHERE customerId = '${existCustomerId}' AND customerAddress = '${data.address}'`;
                        pool.query(sql_query_chkAddress, (err, chkAdd) => {
                            if (err) {
                                console.error("An error occurred in SQL Queery", err);
                                return res.status(500).send('Database Error');
                            } else {
                                if (chkAdd && chkAdd.length) {
                                    return res.status(400).send('Address Already Exisy For This Customer Id');
                                } else {
                                    sql_query_addExistCustomerNewAddress = `INSERT INTO billing_customerAddress_data(addressId, customerId, customerAddress, customerLocality)
                                                                            VALUES('${newAddressId}', '${existCustomerId}', TRIM('${data.address}'), ${data.locality ? `TRIM('${data.locality}')` : null})`;
                                    pool.query(sql_query_addExistCustomerNewAddress, (err, newAdd) => {
                                        if (err) {
                                            console.error("An error occurred in SQL Queery", err);
                                            return res.status(500).send('Database Error');
                                        } else {
                                            return res.status(200).send('Customer New Address Added Successfully');
                                        }
                                    })
                                }
                            }
                        })
                    } else {
                        return res.status(200).send('Customer Added Successfully');
                    }
                } else {
                    sql_query_addNewCustomer = `INSERT INTO billing_customer_data(customerId, customerName, customerMobileNumber, birthDate, anniversaryDate)
                                                VALUES('${newCustometId}', ${data.name ? `TRIM('${data.name}')` : null}, '${data.mobileNo}', NULL, NULL)`;
                    pool.query(sql_query_addNewCustomer, (err, customer) => {
                        if (err) {
                            console.error("An error occurred in SQL Queery", err);
                            return res.status(500).send('Database Error');
                        } else {
                            if (data.address) {
                                sql_query_addCustomerNewAddress = `INSERT INTO billing_customerAddress_data(addressId, customerId, customerAddress, customerLocality)
                                                                   VALUES('${newAddressId}', '${newCustometId}', TRIM('${data.address}'), ${data.locality ? `TRIM('${data.locality}')` : null})`;
                                pool.query(sql_query_addCustomerNewAddress, (err, adds) => {
                                    if (err) {
                                        console.error("An error occurred in SQL Queery", err);
                                        return res.status(500).send('Database Error');
                                    } else {
                                        return res.status(200).send('Customer Added Successfully');
                                    }
                                })
                            } else {
                                return res.status(200).send('Customer Added Success Fully');
                            }
                        }
                    })
                }
            }
        })
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).json('Internal Server Error');
    }
}

// Add Cusstomer Data

const addCustomerData = (req, res) => {
    pool2.getConnection((err, connection) => {
        if (err) {
            console.log('Connection Error', err);
            return res.status(500).send('Database Connection Error');
        }

        try {
            connection.beginTransaction((err) => {
                if (err) {
                    console.log('Error In Transaction');
                    return res.status(500).send('Transaction Error');
                }

                const customerData = req.body;
                const addressJson = customerData?.addressDetails || [];

                const uid1 = new Date();
                const customerId = "customer_" + uid1.getTime();

                if (!customerData.mobileNumber) {
                    connection.rollback(() => {
                        connection.release();
                        return res.status(404).send('Please Fill Mobile Number..!');
                    });
                    return;
                }

                if (addressJson.length && getAddressValidation(addressJson)) {
                    connection.rollback(() => {
                        connection.release();
                        return res.status(400).send('You Can Not Add Same Address');
                    });
                    return;
                }

                let sql_checkExist = `SELECT customerId FROM billing_customer_data WHERE customerMobileNumber = ?`;
                connection.query(sql_checkExist, [customerData.mobileNumber], (err, result) => {
                    if (err) {
                        console.error("Error Find Exist Customer :", err);
                        connection.rollback(() => {
                            connection.release();
                            return res.status(500).send('Database Error');
                        });
                        return;
                    }

                    if (result.length > 0) {
                        connection.rollback(() => {
                            connection.release();
                            return res.status(400).send('Customer Is Already Exist..!');
                        });
                        return;
                    }

                    const sql_addCustomer = `INSERT INTO billing_customer_data (customerId, customerName, customerMobileNumber, birthDate, anniversaryDate)
                                             VALUES (?, TRIM(?), ?, ${customerData.birthDate ? `STR_TO_DATE(?, '%b %d %Y')` : `NULL`}, ${customerData.anniversaryDate ? `STR_TO_DATE(?, '%b %d %Y')` : `NULL`})`;

                    const params = [
                        customerId,
                        customerData.customerName || null,
                        customerData.mobileNumber
                    ];

                    if (customerData.birthDate) params.push(customerData.birthDate);
                    if (customerData.anniversaryDate) params.push(customerData.anniversaryDate);

                    connection.query(sql_addCustomer, params, (err) => {
                        if (err) {
                            console.error("Error Insert Customer Data :", err);
                            connection.rollback(() => {
                                connection.release();
                                return res.status(500).send('Database Error');
                            });
                            return;
                        }

                        if (!addressJson.length) {
                            connection.commit((err) => {
                                if (err) {
                                    console.error("Error committing:", err);
                                    connection.rollback(() => {
                                        connection.release();
                                        return res.status(500).send('Database Error');
                                    });
                                } else {
                                    connection.release();
                                    return res.status(200).send("Customer Added Successfully..!");
                                }
                            });
                            return;
                        }

                        const addressValues = addressJson.map((item, i) => [
                            `addressId_${Date.now() + i}`,
                            customerId,
                            item.address ? item.address.trim() : null,
                            item.locality ? item.locality.trim() : null
                        ]);

                        const sql_addAddress = `INSERT INTO billing_customerAddress_data (addressId, customerId, customerAddress, customerLocality) VALUES ?`;
                        connection.query(sql_addAddress, [addressValues], (err) => {
                            if (err) {
                                console.error("Error Insert Address Data :", err);
                                connection.rollback(() => {
                                    connection.release();
                                    return res.status(500).send('Database Error');
                                });
                                return;
                            }
                            connection.commit((err) => {
                                if (err) {
                                    console.error("Commit Error:", err);
                                    connection.rollback(() => {
                                        connection.release();
                                        return res.status(500).send('Database Error');
                                    });
                                } else {
                                    connection.release();
                                    return res.status(200).send("Customer Added Successfully..!");
                                }
                            });
                        });
                    });
                });
            });
        } catch (error) {
            connection.rollback(() => {
                console.error('Unexpected Error:', error);
                connection.release();
                return res.status(500).send('Internal Server Error');
            });
        }
    });
};

// Remove Customer Data

const removeCustomeData = (req, res) => {
    pool2.getConnection((err, connection) => {
        if (err) {
            console.log('Connection Error', err);
            return res.status(500).send('Database Connection Error');
        }
        try {
            connection.beginTransaction((err) => {
                if (err) {
                    console.log('Error In Transaction', err);
                    return res.status(500).send('Transaction Error');
                }

                const customerId = req.query.customerId || null;

                if (!customerId) {
                    connection.rollback(() => {
                        connection.release();
                        return res.status(404).send('customerId Not Found..!');
                    });
                } else {
                    const sql_check = `SELECT customerId FROM billing_customer_data WHERE customerId = ?`;
                    connection.query(sql_check, [customerId], (err, result) => {
                        if (err) {
                            console.error("Error Find Exist Customer :", err);
                            connection.rollback(() => {
                                connection.release();
                                return res.status(500).send('Database Error');
                            });
                        } else if (!result || result.length === 0) {
                            connection.rollback(() => {
                                connection.release();
                                return res.status(400).send('customerId Not Exist..!');
                            });

                        } else {
                            const sql_delete_customer = `DELETE FROM billing_customer_data WHERE customerId = ?`;
                            connection.query(sql_delete_customer, [customerId], (err) => {
                                if (err) {
                                    console.error("Error Remove Customer Data :", err);
                                    connection.rollback(() => {
                                        connection.release();
                                        return res.status(500).send('Database Error');
                                    });
                                } else {
                                    const sql_delete_address = `DELETE FROM billing_customerAddress_data WHERE customerId = ?`;
                                    connection.query(sql_delete_address, [customerId], (err) => {
                                        if (err) {
                                            console.error("Error Remove Customer Address Data :", err);
                                            connection.rollback(() => {
                                                connection.release();
                                                return res.status(500).send('Database Error');
                                            });
                                        } else {
                                            connection.commit((err) => {
                                                if (err) {
                                                    console.error("Error committing transaction:", err);
                                                    connection.rollback(() => {
                                                        connection.release();
                                                        return res.status(500).send('Database Error');
                                                    });
                                                } else {
                                                    connection.release();
                                                    return res.status(200).send("Customer Removed Successfully..!");
                                                }
                                            });
                                        }
                                    });
                                }
                            });
                        }
                    });
                }
            });
        } catch (error) {
            connection.rollback(() => {
                console.error('An error occurred', error);
                connection.release();
                return res.status(500).json('Internal Server Error');
            });
        }
    });
};

// Update Customer Data

const updateCustomerData = (req, res) => {
    pool2.getConnection((err, connection) => {
        if (err) return res.status(500).send("Database Connection Error");

        try {
            connection.beginTransaction((err) => {
                if (err) return res.status(500).send("Transaction Error");

                const customerData = req.body;
                const addressJson = customerData?.addressDetails || [];

                if (!customerData.mobileNumber) {
                    return connection.rollback(() => {
                        connection.release();
                        return res.status(400).send("Please Fill Mobile Number..!");
                    });
                }

                if (addressJson && getAddressValidation(addressJson)) {
                    return connection.rollback(() => {
                        connection.release();
                        return res.status(400).send("You Can Not Add Same Address");
                    });
                }

                const sqlCheckMobile = `SELECT customerId FROM billing_customer_data
                                        WHERE customerMobileNumber = ? AND customerId != ?`;

                connection.query(
                    sqlCheckMobile,
                    [customerData.mobileNumber, customerData.customerId],
                    (err, result) => {
                        if (err) {
                            console.error("Database :", err);
                            return connection.rollback(() => {
                                connection.release();
                                return res.status(500).send("Database Error");
                            });
                        }

                        if (result.length) {
                            return connection.rollback(() => {
                                connection.release();
                                return res.status(400).send("Customer Mobile No Is Already Exist..!");
                            });
                        }

                        const sqlUpdateCustomer = `
                            UPDATE billing_customer_data SET
                                customerName = TRIM(?),
                                customerMobileNumber = ?,
                                birthDate = ${customerData.birthDate ? "STR_TO_DATE(?, '%b %d %Y')" : "NULL"},
                                anniversaryDate = ${customerData.anniversaryDate ? "STR_TO_DATE(?, '%b %d %Y')" : "NULL"}
                            WHERE customerId = ?
                        `;

                        const paramsCustomer = [
                            customerData.customerName || null,
                            customerData.mobileNumber,
                        ];

                        if (customerData.birthDate) paramsCustomer.push(customerData.birthDate);
                        if (customerData.anniversaryDate) paramsCustomer.push(customerData.anniversaryDate);

                        paramsCustomer.push(customerData.customerId);

                        connection.query(sqlUpdateCustomer, paramsCustomer, (err) => {
                            if (err) {
                                return connection.rollback(() => {
                                    connection.release();
                                    return res.status(500).send("Database Error");
                                });
                            }

                            const sqlFetchOld = `SELECT addressId, customerAddress AS address, customerLocality AS locality
                                                 FROM billing_customerAddress_data
                                                 WHERE customerId = ?`;

                            connection.query(sqlFetchOld, [customerData.customerId], (err, oldAddresses) => {
                                if (err) {
                                    return connection.rollback(() => {
                                        connection.release();
                                        return res.status(500).send("Database Error");
                                    });
                                }

                                const oldMap = {};
                                oldAddresses.forEach(a => oldMap[a.addressId] = a);

                                let toInsert = [];
                                let toUpdate = [];
                                let toDelete = [];

                                // ⭐COMPARE NEW VS OLD
                                addressJson.forEach((item, index) => {
                                    if (item.addressId) {
                                        if (oldMap[item.addressId]) {
                                            toUpdate.push(item);
                                            delete oldMap[item.addressId];
                                        }
                                    } else {
                                        item.addressId = "addressId_" + Date.now() + "_" + index;
                                        toInsert.push(item);
                                    }
                                });

                                toDelete = Object.keys(oldMap);
                                const doDelete = (cb) => {
                                    if (!toDelete.length) return cb();

                                    const sqlDelete = `DELETE FROM billing_customerAddress_data
                                                       WHERE addressId IN (?)`;

                                    connection.query(sqlDelete, [toDelete], (err) => {
                                        if (err) {
                                            return connection.rollback(() => {
                                                connection.release();
                                                return res.status(500).send("Database Error");
                                            });
                                        }
                                        cb();
                                    });
                                };

                                const doUpdate = (cb) => {
                                    if (!toUpdate.length) return cb();

                                    let processed = 0;

                                    toUpdate.forEach(item => {
                                        const sqlUpdateAddr = `UPDATE 
                                                                billing_customerAddress_data 
                                                               SET
                                                                customerAddress = TRIM(?),
                                                                customerLocality = TRIM(?)
                                                               WHERE addressId = ?`;

                                        connection.query(sqlUpdateAddr, [
                                            item.address || null,
                                            item.locality || null,
                                            item.addressId
                                        ], (err) => {
                                            if (err) {
                                                return connection.rollback(() => {
                                                    connection.release();
                                                    return res.status(500).send("Database Error");
                                                });
                                            }

                                            if (++processed === toUpdate.length) cb();
                                        });
                                    });
                                };

                                const doInsert = (cb) => {
                                    if (!toInsert.length) return cb();

                                    const values = toInsert.map(a => [
                                        a.addressId,
                                        customerData.customerId,
                                        a.address ? a.address.trim() : null,
                                        a.locality ? a.locality.trim() : null
                                    ]);

                                    const sqlInsert = `INSERT INTO billing_customerAddress_data(addressId, customerId, customerAddress, customerLocality)
                                                       VALUES ?`;

                                    connection.query(sqlInsert, [values], (err) => {
                                        if (err) {
                                            return connection.rollback(() => {
                                                connection.release();
                                                return res.status(500).send("Database Error");
                                            });
                                        }
                                        cb();
                                    });
                                };

                                doDelete(() => {
                                    doUpdate(() => {
                                        doInsert(() => {
                                            connection.commit((err) => {
                                                if (err) {
                                                    return connection.rollback(() => {
                                                        connection.release();
                                                        return res.status(500).send("Database Error");
                                                    });
                                                }
                                                connection.release();
                                                return res.status(200).send("Customer Updated Successfully..!");
                                            });
                                        });
                                    });
                                });

                            });
                        });
                    }
                );
            });
        } catch (error) {
            console.error("Unexpected Error:", error);
            connection.rollback(() => {
                connection.release();
                return res.status(500).send("Internal Server Error");
            });
        }
    });
};

module.exports = {
    searchCustomerData,
    addMultipleCustomerData,
    addCustomerData,
    removeCustomeData,
    updateCustomerData,
    getCustomerList,
    getCustomerDetailsById,
    getStaticsByCustomer,
    getBillDataBycustomerId
}