const pool = require('../../database');
const pool2 = require('../../databasePool');
const jwt = require("jsonwebtoken");

function getAddressValidation(addressDetails) {

    const addressCount = addressDetails.reduce((acc, curr) => {
        const address = curr.address.toLowerCase(); // Normalize address to lowercase
        acc[address] = (acc[address] || 0) + 1; // Increment count
        return acc;
    }, {});

    const isAddressRepeated = Object.values(addressCount).some(count => count > 1);

    return isAddressRepeated;
}

// Search Customer Data

const searchCustomerData = (req, res) => {
    try {
        let token;
        token = req.headers ? req.headers.authorization.split(" ")[1] : null;
        if (token) {
            const decoded = jwt.verify(token, process.env.JWT_SECRET);
            const branchId = decoded.id.branchId;
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
                                              WHERE  bcd.branchId = '${branchId}' AND bcd.customerMobileNumber LIKE '%` + searchWord + `%'`;
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
        } else {
            return res.status(404).send('Please Login First....!');
        }
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).json('Internal Server Error');
    }
};

// Get Customer Data

const getCustomerList = (req, res) => {
    try {
        let token;
        token = req.headers ? req.headers.authorization.split(" ")[1] : null;
        if (token) {
            const decoded = jwt.verify(token, process.env.JWT_SECRET);
            const branchId = decoded.id.branchId;
            const page = req.query.page;
            const numPerPage = req.query.numPerPage;
            const skip = (page - 1) * numPerPage;
            const limit = skip + ',' + numPerPage;
            sql_querry_getCountDetails = `SELECT count(*) as numRows FROM billing_customer_data WHERE branchId = '${branchId}'`;
            pool.query(sql_querry_getCountDetails, (err, rows, fields) => {
                if (err) {
                    console.error("An error occurred in SQL Queery", err);
                    return res.status(500).send('Database Error');
                } else {
                    const numRows = rows[0].numRows;
                    const numPages = Math.ceil(numRows / numPerPage);
                    const sql_query_getDetails = `SELECT 
                                                    customerId,
                                                    customerName,
                                                    customerMobileNumber
                                                  FROM 
                                                    billing_customer_data
                                                  WHERE branchId = '${branchId}'
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
        } else {
            return res.status(404).send('Please Login First....!');
        }
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
                                             customerId,
                                             customerName,
                                             customerMobileNumber,
                                             birthDate,
                                             anniversaryDate
                                         FROM
                                             billing_customer_data
                                         WHERE customerId = '${customerId}';
                                         SELECT
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
                    let json = {
                        ...data[0][0],
                        addressDetails: data[1]
                    }
                    return res.status(200).send(json);
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
            mobileNo: req.body.mobileNo ? req.body.mobileNo : null,
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
            console.log('Connection Error', err)
            return res.status(500).send('Database Connection Error');
        }
        try {
            connection.beginTransaction((err) => {
                if (err) {
                    console.log('Error In Transaction');
                    return res.status(500).send('Transaction Error');
                }
                let token;
                token = req.headers ? req.headers.authorization.split(" ")[1] : null;
                if (token) {
                    const decoded = jwt.verify(token, process.env.JWT_SECRET);
                    const branchId = decoded.id.branchId;

                    const customerData = req.body;
                    const addressJson = customerData && customerData.addressDetails ? customerData.addressDetails : null;

                    const uid1 = new Date();
                    const customerId = String("customer_" + uid1.getTime());

                    if (!customerData.mobileNumber) {
                        connection.rollback(() => {
                            connection.release();
                            return res.status(404).send('Please Fill Mobile Number..!');
                        })
                    } else if (addressJson && getAddressValidation(addressJson)) {
                        connection.rollback(() => {
                            connection.release();
                            return res.status(400).send('You Can Not Add Same Address');
                        })
                    } else {
                        let sql_query_chkExistCustomer = `SELECT customerId FROM billing_customer_data WHERE customerMobileNumber = '${customerData.mobileNumber}' AND branchId = '${branchId}'`;
                        connection.query(sql_query_chkExistCustomer, (err, result) => {
                            if (err) {
                                console.error("Error Find Exist Customer :", err);
                                connection.rollback(() => {
                                    connection.release();
                                    return res.status(500).send('Database Error');
                                });
                            } else {
                                if (result && result.length) {
                                    connection.rollback(() => {
                                        connection.release();
                                        return res.status(400).send('Customer Is Already Exist..!');
                                    })
                                } else {
                                    let sql_query_addCustomerDetails = `INSERT INTO billing_customer_data (customerId, branchId, customerName, customerMobileNumber, birthDate, anniversaryDate)
                                                                        VALUES ('${customerId}', '${branchId}', ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, '${customerData.mobileNumber}', ${customerData.birthDate ? `STR_TO_DATE('${customerData.birthDate}','%b %d %Y')` : null}, ${customerData.anniversaryDate ? `STR_TO_DATE('${customerData.anniversaryDate}','%b %d %Y')` : null})`;
                                    connection.query(sql_query_addCustomerDetails, (err, add) => {
                                        if (err) {
                                            console.error("Error Insert Customer Data :", err);
                                            connection.rollback(() => {
                                                connection.release();
                                                return res.status(500).send('Database Error');
                                            });
                                        } else {
                                            if (customerData.addressDetails && customerData.addressDetails.length) {
                                                let addAddressData = addressJson.map((item, index) => {
                                                    let uniqueId = `addressId_${Date.now() + index}`; // Generating a unique ID using current timestamp
                                                    return `('${uniqueId}', '${customerId}', ${item.address ? `TRIM('${item.address}')` : null}, ${item.locality ? `TRIM('${item.locality}')` : null})`;
                                                }).join(', ');
                                                let sql_query_addAddressData = `INSERT INTO billing_customerAddress_data(addressId, customerId, customerAddress, customerLocality)
                                                                                VALUES ${addAddressData}`;
                                                connection.query(sql_query_addAddressData, (err, add) => {
                                                    if (err) {
                                                        console.error("Error Insert Address Data :", err);
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
                                                                return res.status(200).send("Customer Added Successfully..!");
                                                            }
                                                        });
                                                    }
                                                })
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
                                                        return res.status(200).send("Customer Added Successfully..!");
                                                    }
                                                });
                                            }
                                        }
                                    })
                                }
                            }
                        });
                    }
                } else {
                    connection.rollback(() => {
                        connection.release();
                        return res.status(404).send('Please Login First....!');
                    });
                }
            })
        } catch (error) {
            connection.rollback(() => {
                console.error('An error occurred', error);
                connection.release();
                return res.status(500).json('Internal Server Error');
            })
        }
    })
}

// Update Customer Data

const updateCustomerData = (req, res) => {
    pool2.getConnection((err, connection) => {
        if (err) {
            console.log('Connection Error', err)
            return res.status(500).send('Database Connection Error');
        }
        try {
            connection.beginTransaction((err) => {
                if (err) {
                    console.log('Error In Transaction');
                    return res.status(500).send('Transaction Error');
                }
                let token;
                token = req.headers ? req.headers.authorization.split(" ")[1] : null;
                if (token) {
                    const decoded = jwt.verify(token, process.env.JWT_SECRET);
                    const branchId = decoded.id.branchId;

                    const customerData = req.body;
                    const addressJson = customerData && customerData.addressDetails ? customerData.addressDetails : null;

                    if (!customerData.mobileNumber) {
                        connection.rollback(() => {
                            connection.release();
                            return res.status(404).send('Please Fill Mobile Number..!');
                        })
                    } else if (addressJson && getAddressValidation(addressJson)) {
                        connection.rollback(() => {
                            connection.release();
                            return res.status(400).send('You Can Not Add Same Address');
                        })
                    } else {
                        let sql_query_chkExistCustomer = `SELECT customerId FROM billing_customer_data WHERE customerMobileNumber = '${customerData.mobileNumber}' AND branchId = '${branchId}' AND customerId != '${customerData.customerId}'`;
                        connection.query(sql_query_chkExistCustomer, (err, result) => {
                            if (err) {
                                console.error("Error Find Exist Customer :", err);
                                connection.rollback(() => {
                                    connection.release();
                                    return res.status(500).send('Database Error');
                                });
                            } else {
                                if (result && result.length) {
                                    connection.rollback(() => {
                                        connection.release();
                                        return res.status(400).send('Customer Mobile No Is Already Exist..!');
                                    })
                                } else {
                                    let sql_query_addCustomerDetails = `UPDATE
                                                                            billing_customer_data
                                                                        SET
                                                                            customerName = ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null},
                                                                            customerMobileNumber = '${customerData.mobileNumber}',
                                                                            birthDate = ${customerData.birthDate ? `STR_TO_DATE('${customerData.birthDate}','%b %d %Y')` : null},
                                                                            anniversaryDate = ${customerData.anniversaryDate ? `STR_TO_DATE('${customerData.anniversaryDate}','%b %d %Y')` : null}
                                                                        WHERE customerId = '${customerData.customerId}'`
                                    connection.query(sql_query_addCustomerDetails, (err, add) => {
                                        if (err) {
                                            console.error("Error Insert Customer Data :", err);
                                            connection.rollback(() => {
                                                connection.release();
                                                return res.status(500).send('Database Error');
                                            });
                                        } else {
                                            let sql_query_removeOldAddress = `DELETE FROM billing_customerAddress_data WHERE customerId = '${customerData.customerId}'`;
                                            connection.query(sql_query_removeOldAddress, (err, add) => {
                                                if (err) {
                                                    console.error("Error Removing Old Customer Address :", err);
                                                    connection.rollback(() => {
                                                        connection.release();
                                                        return res.status(500).send('Database Error');
                                                    });
                                                } else {
                                                    if (customerData.addressDetails && customerData.addressDetails.length) {
                                                        let addAddressData = addressJson.map((item, index) => {
                                                            let uniqueId = `addressId_${Date.now() + index}`; // Generating a unique ID using current timestamp
                                                            return `('${uniqueId}', '${customerData.customerId}', ${item.address ? `TRIM('${item.address}')` : null}, ${item.locality ? `TRIM('${item.locality}')` : null})`;
                                                        }).join(', ');
                                                        let sql_query_addAddressData = `INSERT INTO billing_customerAddress_data(addressId, customerId, customerAddress, customerLocality)
                                                                                        VALUES ${addAddressData}`;
                                                        connection.query(sql_query_addAddressData, (err, add) => {
                                                            if (err) {
                                                                console.error("Error Insert Address Data :", err);
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
                                                                        return res.status(200).send("Customer Updated Successfully..!");
                                                                    }
                                                                });
                                                            }

                                                        })
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
                                                                return res.status(200).send("Customer Updated Successfully..!");
                                                            }
                                                        });
                                                    }
                                                }
                                            })
                                        }
                                    })
                                }
                            }
                        });
                    }
                } else {
                    connection.rollback(() => {
                        connection.release();
                        return res.status(404).send('Please Login First....!');
                    });
                }
            })
        } catch (error) {
            connection.rollback(() => {
                console.error('An error occurred', error);
                connection.release();
                return res.status(500).json('Internal Server Error');
            })
        }
    })
}

module.exports = {
    searchCustomerData,
    addMultipleCustomerData,
    addCustomerData,
    updateCustomerData,
    getCustomerList,
    getCustomerDetailsById
}