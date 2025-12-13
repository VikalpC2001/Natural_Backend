const pool = require('../../database');
const jwt = require("jsonwebtoken");
const pool2 = require('../../databasePool');

// Get Date Function 4 Hour

function getCurrentDate() {
    const now = new Date();
    const hours = now.getHours();

    if (hours <= 4) { // If it's 4 AM or later, increment the date
        now.setDate(now.getDate() - 1);
    }
    return now.toDateString().slice(4, 15);
}

// Get Number Of Hold

const getHoldCount = (req, res) => {
    try {
        let token;
        token = req.headers ? req.headers.authorization.split(" ")[1] : null;
        if (token) {
            const decoded = jwt.verify(token, process.env.JWT_SECRET);
            const branchId = decoded.id.branchId;
            let sql_query_getHoldNumber = `SELECT COUNT(*) AS holdNo FROM hold_data WHERE branchId = '${branchId}'`;
            pool.query(sql_query_getHoldNumber, (err, data) => {
                if (err) {
                    console.error("An error occurred in SQL Queery", err);
                    return res.status(500).send('Database Error');
                } else {
                    return res.status(200).send(data[0]);
                }
            })
        } else {
            return res.status(400).send('Please Login First....!');
        }
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).json('Internal Server Error');
    }
}

// Get Hold Bill List

const getHoldBillData = (req, res) => {
    try {
        let token;
        token = req.headers ? req.headers.authorization.split(" ")[1] : null;
        if (token) {
            const decoded = jwt.verify(token, process.env.JWT_SECRET);
            const branchId = decoded.id.branchId;
            let sql_query_getHoldBill = `SELECT
                                         hd.holdId AS holdId,
                                         hd.settledAmount AS totalAmount,
                                         hd.cashier AS holdBy,
                                         hd.menuStatus AS orderStatus,
                                         CONCAT(DATE_FORMAT(hd.billDate,'%d-%b-%Y'),' ',DATE_FORMAT(hd.billCreationDate,'%h:%i:%s')) AS holdDateTime,
                                         hd.billType AS billType
                                     FROM
                                         hold_data AS hd
                                     WHERE hd.branchId = '${branchId}'
                                     ORDER BY hd.billCreationDate DESC;`;
            pool.query(sql_query_getHoldBill, (err, data) => {
                if (err) {
                    console.error("An error occurred in SQL Queery", err);
                    return res.status(500).send('Database Error');
                } else {
                    if (data && data.length) {
                        return res.status(200).send(data);
                    } else {
                        return res.status(404).send('No Data Found');
                    }
                }
            })
        } else {
            return res.status(400).send('Please Login First....!');
        }
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).json('Internal Server Error');
    }
}

// Get Hold Data By Id

const getHoldBillDataById = (req, res) => {
    try {
        let token;
        token = req.headers ? req.headers.authorization.split(" ")[1] : null;
        if (token) {
            const decoded = jwt.verify(token, process.env.JWT_SECRET);
            const branchId = decoded.id.branchId;
            const holdId = req.query.holdId;
            if (!holdId) {
                return res.status(404).send('holdId Not Found');
            } else {
                let sql_query_chkBillExist = `SELECT holdId, billType FROM hold_data WHERE holdId = '${holdId}' AND branchId = '${branchId}'`;
                pool.query(sql_query_chkBillExist, (err, bill) => {
                    if (err) {
                        console.error("An error occurred in SQL Queery", err);
                        return res.status(500).send('Database Error');
                    } else {
                        if (bill && bill.length) {
                            const billType = bill[0].billType;
                            let sql_query_getBillingData = `SELECT 
                                                            hd.holdId AS holdId, 
                                                            hd.firmId AS firmId, 
                                                            hd.cashier AS cashier, 
                                                            hd.menuStatus AS menuStatus, 
                                                            hd.billType AS billType, 
                                                            hd.billPayType AS billPayType, 
                                                            hd.discountType AS discountType, 
                                                            hd.discountValue AS discountValue, 
                                                            hd.totalDiscount AS totalDiscount, 
                                                            hd.totalAmount AS totalAmount, 
                                                            hd.settledAmount AS settledAmount, 
                                                            hd.billComment AS billComment, 
                                                            DATE_FORMAT(hd.billDate,'%d/%m/%Y') AS billDate,
                                                            hd.billStatus AS billStatus,
                                                            DATE_FORMAT(hd.billCreationDate,'%h:%i %p') AS billTime
                                                        FROM 
                                                            hold_data AS hd
                                                        WHERE hd.holdId = '${holdId}'`;
                            let sql_query_getBillwiseItem = `SELECT
                                                                 hwid.iwbId AS iwbId,
                                                                 hwid.itemId AS itemId,
                                                                 imd.itemName AS itemName,
                                                                 imd.itemGujaratiName AS itemGujaratiName,
                                                                 uwp.preferredName AS preferredName,
                                                                 imd.itemCode AS inputCode,
                                                                 hwid.qty AS qty,
                                                                 hwid.unit AS unit,
                                                                 hwid.itemPrice AS itemPrice,
                                                                 hwid.price AS price,
                                                                 hwid.comment AS comment
                                                             FROM
                                                                 hold_billWiseItem_data AS hwid
                                                             INNER JOIN item_menuList_data AS imd ON imd.itemId = hwid.itemId
                                                             LEFT JOIN item_unitWisePrice_data AS uwp ON uwp.itemId = hwid.itemId AND uwp.unit = hwid.unit
                                                             WHERE hwid.holdId = '${holdId}'`;
                            let sql_query_getItemWiseAddons = `SELECT
                                                                   iwad.iwaId AS iwaId,
                                                                   iwad.iwbId AS iwbId,
                                                                   iwad.addOnsId AS addOnsId,
                                                                   iad.addonsName AS addonsName,
                                                                   iad.price AS addonPrice
                                                               FROM
                                                                   hold_itemWiseAddon_data AS iwad
                                                               LEFT JOIN item_addons_data AS iad ON iad.addonsId = iwad.addOnsId
                                                               WHERE iwad.iwbId IN(SELECT COALESCE(hbwid.iwbId, NULL) FROM hold_billWiseItem_data AS hbwid WHERE hbwid.holdId = '${holdId}')`;
                            let sql_query_getCustomerInfo = `SELECT
                                                             hwcd.bwcId AS bwcId,
                                                             hwcd.customerId AS customerId,
                                                             hwcd.mobileNo AS mobileNo,
                                                             hwcd.addressId AS addressId,
                                                             hwcd.address AS address,
                                                             hwcd.locality AS locality,
                                                             hwcd.customerName AS customerName
                                                         FROM
                                                             hold_billWiseCustomer_data AS hwcd
                                                         WHERE hwcd.holdId = '${holdId}'`;
                            let sql_query_getFirmData = `SELECT 
                                                           firmId, 
                                                           firmName, 
                                                           gstNumber, 
                                                           firmAddress, 
                                                           pincode, 
                                                           firmMobileNo, 
                                                           otherMobileNo 
                                                        FROM 
                                                           billing_firm_data 
                                                        WHERE 
                                                           firmId = (SELECT firmId FROM hold_data WHERE holdId = '${holdId}')`
                            const sql_query_getBillData = `${sql_query_getBillingData};
                                                           ${sql_query_getBillwiseItem};
                                                           ${sql_query_getFirmData};
                                                           ${sql_query_getItemWiseAddons};
                                                           ${billType == 'Pick Up' || billType == 'Delivery' ? sql_query_getCustomerInfo : ''}`;
                            pool.query(sql_query_getBillData, (err, billData) => {
                                if (err) {
                                    console.error("An error occurred in SQL Queery", err);
                                    return res.status(500).send('Database Error'); t
                                } else {
                                    const itemsData = billData && billData[1] ? billData[1] : [];
                                    const addonsData = billData && billData[3] ? billData[3] : [];

                                    const newItemJson = itemsData.map(item => {
                                        const itemAddons = addonsData.filter(addon => addon.iwbId === item.iwbId);
                                        return {
                                            ...item,
                                            addons: Object.fromEntries(itemAddons.map(addon => [addon.addOnsId, addon])),
                                            addonPrice: itemAddons.reduce((sum, { price }) => sum + price, 0)
                                        };
                                    });

                                    const json = {
                                        ...billData[0][0],
                                        itemData: newItemJson,
                                        firmData: billData && billData[2] ? billData[2][0] : [],
                                        ...(billType == 'Pick Up' || billType == 'Delivery' ? { customerDetails: billData && billData[4][0] ? billData[4][0] : '' } : '')
                                    }
                                    const holdJson = json;
                                    let sql_query_discardData = `DELETE FROM hold_data WHERE holdId = '${holdId}';
                                                                 DELETE FROM hold_billWiseItem_data WHERE holdId = '${holdId}';
                                                                 DELETE FROM hold_billWiseCustomer_data WHERE holdId = '${holdId}'`;
                                    pool.query(sql_query_discardData, (err, data) => {
                                        if (err) {
                                            console.error("An error occurred in SQL Queery", err);
                                            return res.status(500).send('Database Error');
                                        } else {
                                            let sql_query_getHoldNumber = `SELECT COUNT(*) AS holdNo FROM hold_data WHERE branchId = '${branchId}'`;
                                            pool.query(sql_query_getHoldNumber, (err, data) => {
                                                if (err) {
                                                    console.error("An error occurred in SQL Queery", err);
                                                    return res.status(500).send('Database Error');
                                                } else {
                                                    const holdCount = data && data[0] ? data[0].holdNo : 0;
                                                    req?.io?.emit(`getHoldCount_${branchId}`, holdCount);
                                                    return res.status(200).send(holdJson);
                                                }
                                            })
                                        }
                                    })
                                }
                            })
                        } else {
                            return res.status(404).send('Hold Id Not Found');
                        }
                    }
                })
            }
        } else {
            return res.status(400).send('Please Login First....!');
        }
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).json('Internal Server Error');
    }
}

// Add PickUp Bill Data

const addPickUpHoldBillData = (req, res) => {
    pool2.getConnection((err, connection) => {
        if (err) {
            console.error("Error getting database connection:", err);
            return res.status(500).send('Database Error');
        }
        try {
            connection.beginTransaction((err) => {
                if (err) {
                    console.error("Error beginning transaction:", err);
                    connection.release();
                    return res.status(500).send('Database Error');
                } else {
                    let token;
                    token = req.headers ? req.headers.authorization.split(" ")[1] : null;
                    if (token) {
                        const decoded = jwt.verify(token, process.env.JWT_SECRET);
                        const cashier = decoded.id.firstName;
                        const branchId = decoded.id.branchId;

                        const currentDate = getCurrentDate();
                        const holdData = req.body;
                        if (!holdData.customerDetails || !branchId || !holdData.firmId || !holdData.subTotal || !holdData.settledAmount || !holdData.billPayType || !holdData.billStatus || !holdData.itemsData) {
                            connection.rollback(() => {
                                connection.release();
                                return res.status(404).send('Please Fill All The Fields..!');
                            })
                        } else {

                            const uid1 = new Date();
                            const holdId = String("hold_" + uid1.getTime());
                            const bwcId = String("bwc_" + uid1.getTime());
                            const newCustometId = String("customer_" + uid1.getTime());
                            const newAddressId = String("addressId_" + uid1.getTime());

                            const columnData = `holdId,
                                                firmId,
                                                branchId,
                                                cashier,
                                                menuStatus,
                                                billType,
                                                billPayType,
                                                discountType,
                                                discountValue,
                                                totalDiscount,
                                                totalAmount,
                                                settledAmount,
                                                billComment,
                                                billDate,
                                                billStatus`;
                            const values = `'${holdId}',
                                            '${holdData.firmId}',
                                            '${branchId}', 
                                            '${cashier}', 
                                            'Offline',
                                            'Pick Up',
                                            '${holdData.billPayType}',
                                            '${holdData.discountType}',
                                            ${holdData.discountValue},
                                            ${holdData.totalDiscount},
                                            ${holdData.subTotal},
                                            ${holdData.settledAmount},
                                            ${holdData.billComment ? `'${holdData.billComment}'` : null},
                                            STR_TO_DATE('${currentDate}','%b %d %Y'),
                                            '${holdData.billStatus}'`;

                            let sql_querry_addHoldBillInfo = `INSERT INTO hold_data (${columnData}) VALUES (${values})`;
                            connection.query(sql_querry_addHoldBillInfo, (err) => {
                                if (err) {
                                    console.error("Error inserting new bill number:", err);
                                    connection.rollback(() => {
                                        connection.release();
                                        return res.status(500).send('Database Error');
                                    });
                                } else {
                                    const billItemData = holdData.itemsData

                                    const addBillWiseItemData = [];
                                    const addItemWiseAddonData = [];

                                    billItemData.forEach((item, index) => {
                                        let uniqueId = `iwb_${Date.now() + index}_${index}`; // Unique ID generation

                                        // Construct SQL_Add_1 for the main item
                                        addBillWiseItemData.push(`('${uniqueId}', '${holdId}', '${branchId}', '${item.itemId}', ${item.qty}, '${item.unit}', ${item.itemPrice}, ${item.price}, ${item.comment ? `'${item.comment}'` : null})`);

                                        // Construct SQL_Add_2 for the addons
                                        const allAddons = item.addons ? Object.keys(item.addons) : []
                                        if (allAddons && allAddons.length) {
                                            allAddons.forEach((addonId, addonIndex) => {
                                                let iwaId = `iwa_${Date.now() + addonIndex + index}_${index}`; // Unique ID for each addon
                                                addItemWiseAddonData.push(`('${iwaId}', '${uniqueId}', '${addonId}')`);
                                            });
                                        }
                                    });
                                    let sql_query_addItems = `INSERT INTO hold_billWiseItem_data(iwbId, holdId, branchId, itemId, qty, unit, itemPrice, price, comment)
                                                              VALUES ${addBillWiseItemData.join(", ")}`;
                                    connection.query(sql_query_addItems, (err) => {
                                        if (err) {
                                            console.error("Error inserting Bill Wise Item Data:", err);
                                            connection.rollback(() => {
                                                connection.release();
                                                return res.status(500).send('Database Error');
                                            });
                                        } else {
                                            let sql_query_getHoldCount = `SELECT COUNT(*) AS holdNo FROM hold_data WHERE branchId = '${branchId}';
                                                                         ${addItemWiseAddonData.length ? `INSERT INTO hold_itemWiseAddon_data (iwaId, iwbId, addOnsId) VALUES ${addItemWiseAddonData.join(", ")}` : ''}`;
                                            connection.query(sql_query_getHoldCount, (err, count) => {
                                                if (err) {
                                                    console.error("Error inserting Bill Wise Item Data:", err);
                                                    connection.rollback(() => {
                                                        connection.release();
                                                        return res.status(500).send('Database Error');
                                                    });
                                                } else {
                                                    let holdCount = count && count[0] ? addItemWiseAddonData.length ? count[0][0].holdNo : count[0].holdNo : 0;
                                                    const customerData = holdData.customerDetails;
                                                    if (customerData && customerData.customerId && customerData.addressId) {
                                                        let sql_query_addAddressRelation = `INSERT INTO hold_billWiseCustomer_data(bwcId, holdId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                            VALUES ('${bwcId}', '${holdId}', '${customerData.customerId}', '${customerData.addressId}', ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
                                                        connection.query(sql_query_addAddressRelation, (err) => {
                                                            if (err) {
                                                                console.error("Error inserting Customer Bill Wise Data:", err);
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
                                                                        req?.io?.emit(`getHoldCount_${branchId}`, holdCount);
                                                                        return res.status(200).send("Bill Is On Hold");
                                                                    }
                                                                });
                                                            }
                                                        });
                                                    } else if (customerData && customerData.customerId && customerData.address) {
                                                        let sql_queries_chkOldAdd = `SELECT addressId, customerId FROM billing_customerAddress_data WHERE customerAddress = TRIM('${customerData.address}') AND customerLocality = '${customerData.locality}'`;
                                                        connection.query(sql_queries_chkOldAdd, (err, oldAdd) => {
                                                            if (err) {
                                                                console.error("Error inserting Customer New Address:", err);
                                                                connection.rollback(() => {
                                                                    connection.release();
                                                                    return res.status(500).send('Database Error');
                                                                });
                                                            } else {
                                                                if (oldAdd && oldAdd[0]) {
                                                                    const existAddressId = oldAdd[0].addressId;
                                                                    let sql_query_addAddressRelation = `INSERT INTO hold_billWiseCustomer_data(bwcId, holdId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                        VALUES ('${bwcId}', '${holdId}', '${customerData.customerId}', '${existAddressId}', ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
                                                                    connection.query(sql_query_addAddressRelation, (err) => {
                                                                        if (err) {
                                                                            console.error("Error inserting Customer Bill Wise Data:", err);
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
                                                                                    req?.io?.emit(`getHoldCount_${branchId}`, holdCount);
                                                                                    return res.status(200).send("Bill Is On Hold");
                                                                                }
                                                                            });
                                                                        }
                                                                    });
                                                                } else {
                                                                    let sql_querry_addNewAddress = `INSERT INTO billing_customerAddress_data(addressId, customerId, customerAddress, customerLocality)
                                                                                                    VALUES ('${newAddressId}', '${customerData.customerId}', TRIM('${customerData.address}'), ${customerData.locality ? `TRIM('${customerData.locality}')` : null})`;
                                                                    connection.query(sql_querry_addNewAddress, (err) => {
                                                                        if (err) {
                                                                            console.error("Error inserting Customer New Address:", err);
                                                                            connection.rollback(() => {
                                                                                connection.release();
                                                                                return res.status(500).send('Database Error');
                                                                            });
                                                                        } else {
                                                                            let sql_query_addAddressRelation = `INSERT INTO hold_billWiseCustomer_data(bwcId, holdId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                VALUES ('${bwcId}', '${holdId}', '${customerData.customerId}', '${newAddressId}', ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
                                                                            connection.query(sql_query_addAddressRelation, (err) => {
                                                                                if (err) {
                                                                                    console.error("Error inserting Customer Bill Wise Data:", err);
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
                                                                                            req?.io?.emit(`getHoldCount_${branchId}`, holdCount);
                                                                                            return res.status(200).send("Bill Is On Hold");
                                                                                        }
                                                                                    });
                                                                                }
                                                                            });
                                                                        }
                                                                    })
                                                                }
                                                            }
                                                        });
                                                    } else if (customerData && customerData.customerId) {
                                                        let sql_query_addAddressRelation = `INSERT INTO hold_billWiseCustomer_data(bwcId, holdId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                            VALUES ('${bwcId}', '${holdId}', '${customerData.customerId}', ${customerData.addressId ? `'${customerData.addressId}'` : null}, ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
                                                        connection.query(sql_query_addAddressRelation, (err) => {
                                                            if (err) {
                                                                console.error("Error inserting Customer Bill Wise Data:", err);
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
                                                                        req?.io?.emit(`getHoldCount_${branchId}`, holdCount);
                                                                        return res.status(200).send("Bill Is On Hold");
                                                                    }
                                                                });
                                                            }
                                                        });
                                                    } else {
                                                        if (customerData && (customerData.customerName || customerData.mobileNo)) {
                                                            let sql_querry_getExistCustomer = `SELECT customerId, customerMobileNumber FROM billing_customer_data WHERE customerMobileNumber = '${customerData.mobileNo}'`;
                                                            connection.query(sql_querry_getExistCustomer, (err, num) => {
                                                                if (err) {
                                                                    console.error("Error Get Existing Customer Data:", err);
                                                                    connection.rollback(() => {
                                                                        connection.release();
                                                                        return res.status(500).send('Database Error');
                                                                    });
                                                                } else {
                                                                    const existCustomerId = num && num[0] ? num[0].customerId : null;
                                                                    if (existCustomerId && customerData.address) {
                                                                        let sql_queries_chkOldAdd = `SELECT addressId, customerId FROM billing_customerAddress_data WHERE customerAddress = TRIM('${customerData.address}') AND customerLocality = '${customerData.locality}'`;
                                                                        connection.query(sql_queries_chkOldAdd, (err, oldAdd) => {
                                                                            if (err) {
                                                                                console.error("Error inserting Customer New Address:", err);
                                                                                connection.rollback(() => {
                                                                                    connection.release();
                                                                                    return res.status(500).send('Database Error');
                                                                                });
                                                                            } else {
                                                                                if (oldAdd && oldAdd[0]) {
                                                                                    const existAddressId = oldAdd[0].addressId;
                                                                                    let sql_query_addAddressRelation = `INSERT INTO hold_billWiseCustomer_data(bwcId, holdId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                        VALUES ('${bwcId}', '${holdId}', '${existCustomerId}', '${existAddressId}', ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
                                                                                    connection.query(sql_query_addAddressRelation, (err) => {
                                                                                        if (err) {
                                                                                            console.error("Error inserting Customer Bill Wise Data:", err);
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
                                                                                                    req?.io?.emit(`getHoldCount_${branchId}`, holdCount);
                                                                                                    return res.status(200).send("Bill Is On Hold");
                                                                                                }
                                                                                            });
                                                                                        }
                                                                                    });
                                                                                } else {
                                                                                    let sql_querry_addNewAddress = `INSERT INTO billing_customerAddress_data(addressId, customerId, customerAddress, customerLocality)
                                                                                                                    VALUES ('${newAddressId}', '${existCustomerId}', TRIM('${customerData.address}'), ${customerData.locality ? `TRIM('${customerData.locality}')` : null})`;
                                                                                    connection.query(sql_querry_addNewAddress, (err) => {
                                                                                        if (err) {
                                                                                            console.error("Error inserting Customer New Address:", err);
                                                                                            connection.rollback(() => {
                                                                                                connection.release();
                                                                                                return res.status(500).send('Database Error');
                                                                                            });
                                                                                        } else {
                                                                                            let sql_query_addAddressRelation = `INSERT INTO hold_billWiseCustomer_data(bwcId, holdId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                                VALUES ('${bwcId}', '${holdId}', '${existCustomerId}', '${newAddressId}', ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
                                                                                            connection.query(sql_query_addAddressRelation, (err) => {
                                                                                                if (err) {
                                                                                                    console.error("Error inserting Customer Bill Wise Data:", err);
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
                                                                                                            req?.io?.emit(`getHoldCount_${branchId}`, holdCount);
                                                                                                            return res.status(200).send("Bill Is On Hold");
                                                                                                        }
                                                                                                    });
                                                                                                }
                                                                                            });
                                                                                        }
                                                                                    })
                                                                                }
                                                                            }
                                                                        })
                                                                    } else if (customerData.address) {
                                                                        let sql_querry_addNewCustomer = `INSERT INTO billing_customer_data(customerId, customerName, customerMobileNumber, birthDate, anniversaryDate)
                                                                                                         VALUES ('${newCustometId}', ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.mobileNo ? `'${customerData.mobileNo}'` : null}, ${customerData.birthDate ? `STR_TO_DATE('${customerData.birthDate}','%b %d %Y')` : null}, ${customerData.aniversaryDate ? `STR_TO_DATE('${customerData.aniversaryDate}','%b %d %Y')` : null})`;
                                                                        connection.query(sql_querry_addNewCustomer, (err) => {
                                                                            if (err) {
                                                                                console.error("Error inserting New Customer Data:", err);
                                                                                connection.rollback(() => {
                                                                                    connection.release();
                                                                                    return res.status(500).send('Database Error');
                                                                                });
                                                                            } else {
                                                                                let sql_querry_addNewAddress = `INSERT INTO billing_customerAddress_data(addressId, customerId, customerAddress, customerLocality)
                                                                                                                VALUES ('${newAddressId}', '${newCustometId}', TRIM('${customerData.address}'), ${customerData.locality ? `TRIM('${customerData.locality}')` : null})`;
                                                                                connection.query(sql_querry_addNewAddress, (err) => {
                                                                                    if (err) {
                                                                                        console.error("Error inserting Customer New Address:", err);
                                                                                        connection.rollback(() => {
                                                                                            connection.release();
                                                                                            return res.status(500).send('Database Error');
                                                                                        });
                                                                                    } else {
                                                                                        let sql_query_addAddressRelation = `INSERT INTO hold_billWiseCustomer_data(bwcId, holdId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                            VALUES ('${bwcId}', '${holdId}', '${newCustometId}', '${newAddressId}', ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
                                                                                        connection.query(sql_query_addAddressRelation, (err) => {
                                                                                            if (err) {
                                                                                                console.error("Error inserting Customer Bill Wise Data:", err);
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
                                                                                                        req?.io?.emit(`getHoldCount_${branchId}`, holdCount);
                                                                                                        return res.status(200).send("Bill Is On Hold");
                                                                                                    }
                                                                                                });
                                                                                            }
                                                                                        });
                                                                                    }
                                                                                })
                                                                            }
                                                                        })
                                                                    } else if (existCustomerId) {
                                                                        let sql_query_addAddressRelation = `INSERT INTO hold_billWiseCustomer_data(bwcId, holdId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                            VALUES ('${bwcId}', '${holdId}', '${existCustomerId}', NULL, ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
                                                                        connection.query(sql_query_addAddressRelation, (err) => {
                                                                            if (err) {
                                                                                console.error("Error inserting Customer Bill Wise Data:", err);
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
                                                                                        req?.io?.emit(`getHoldCount_${branchId}`, holdCount);
                                                                                        return res.status(200).send("Bill Is On Hold");
                                                                                    }
                                                                                });
                                                                            }
                                                                        });
                                                                    } else if (customerData.mobileNo) {
                                                                        let sql_querry_addNewCustomer = `INSERT INTO billing_customer_data(customerId, customerName, customerMobileNumber, birthDate, anniversaryDate)
                                                                                                         VALUES ('${newCustometId}', ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.mobileNo ? `'${customerData.mobileNo}'` : null}, ${customerData.birthDate ? `STR_TO_DATE('${customerData.birthDate}','%b %d %Y')` : null}, ${customerData.aniversaryDate ? `STR_TO_DATE('${customerData.aniversaryDate}','%b %d %Y')` : null})`;
                                                                        connection.query(sql_querry_addNewCustomer, (err) => {
                                                                            if (err) {
                                                                                console.error("Error inserting New Customer Data:", err);
                                                                                connection.rollback(() => {
                                                                                    connection.release();
                                                                                    return res.status(500).send('Database Error');
                                                                                });
                                                                            } else {
                                                                                let sql_query_addAddressRelation = `INSERT INTO hold_billWiseCustomer_data(bwcId, holdId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                    VALUES ('${bwcId}', '${holdId}', '${newCustometId}', NULL, ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
                                                                                connection.query(sql_query_addAddressRelation, (err) => {
                                                                                    if (err) {
                                                                                        console.error("Error inserting Customer Bill Wise Data:", err);
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
                                                                                                req?.io?.emit(`getHoldCount_${branchId}`, holdCount);
                                                                                                return res.status(200).send("Bill Is On Hold");
                                                                                            }
                                                                                        });
                                                                                    }
                                                                                });
                                                                            }
                                                                        })
                                                                    } else {
                                                                        let sql_query_addAddressRelation = `INSERT INTO hold_billWiseCustomer_data(bwcId, holdId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                            VALUES ('${bwcId}', '${holdId}', NULL, NULL, ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
                                                                        connection.query(sql_query_addAddressRelation, (err) => {
                                                                            if (err) {
                                                                                console.error("Error inserting Customer Bill Wise Data:", err);
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
                                                                                        req?.io?.emit(`getHoldCount_${branchId}`, holdCount);
                                                                                        return res.status(200).send("Bill Is On Hold");
                                                                                    }
                                                                                });
                                                                            }
                                                                        });
                                                                    }
                                                                }
                                                            })
                                                        } else if (customerData.address || customerData.locality) {
                                                            let sql_query_addAddressRelation = `INSERT INTO hold_billWiseCustomer_data(bwcId, holdId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                VALUES ('${bwcId}', '${holdId}', NULL, NULL, ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
                                                            connection.query(sql_query_addAddressRelation, (err) => {
                                                                if (err) {
                                                                    console.error("Error inserting Customer Bill Wise Data:", err);
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
                                                                            req?.io?.emit(`getHoldCount_${branchId}`, holdCount);
                                                                            return res.status(200).send("Bill Is On Hold");
                                                                        }
                                                                    });
                                                                }
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
                                                                    req?.io?.emit(`getHoldCount_${branchId}`, holdCount);
                                                                    return res.status(200).send("Bill Is On Hold");
                                                                }
                                                            });
                                                        }
                                                    }
                                                }
                                            })
                                        }
                                    });
                                }
                            });
                        }
                    } else {
                        connection.rollback(() => {
                            connection.release();
                            return res.status(404).send('Please Login First....!');
                        });
                    }
                }
            });
        } catch (error) {
            console.error('An error occurred', error);
            connection.rollback(() => {
                connection.release();
                return res.status(500).json('Internal Server Error');
            })
        }
    });
}

// Add Delivery Bill Data

const addDeliveryHoldBillData = (req, res) => {
    pool2.getConnection((err, connection) => {
        if (err) {
            console.error("Error getting database connection:", err);
            return res.status(500).send('Database Error');
        }
        try {
            connection.beginTransaction((err) => {
                if (err) {
                    console.error("Error beginning transaction:", err);
                    connection.release();
                    return res.status(500).send('Database Error');
                } else {
                    let token;
                    token = req.headers ? req.headers.authorization.split(" ")[1] : null;
                    if (token) {
                        const decoded = jwt.verify(token, process.env.JWT_SECRET);
                        const cashier = decoded.id.firstName;
                        const branchId = decoded.id.branchId;

                        const currentDate = getCurrentDate();
                        const holdData = req.body;
                        if (!holdData.customerDetails || !holdData.firmId || !holdData.subTotal || !holdData.settledAmount || !holdData.billPayType || !holdData.billStatus || !holdData.itemsData) {
                            connection.rollback(() => {
                                connection.release();
                                return res.status(404).send('Please Fill All The Fields..!');
                            })
                        } else {

                            const uid1 = new Date();
                            const holdId = String("hold_" + uid1.getTime());
                            const bwcId = String("bwc_" + uid1.getTime());
                            const newCustometId = String("customer_" + uid1.getTime());
                            const newAddressId = String("addressId_" + uid1.getTime());

                            const columnData = `holdId,
                                                firmId,
                                                branchId,
                                                cashier,
                                                menuStatus,
                                                billType,
                                                billPayType,
                                                discountType,
                                                discountValue,
                                                totalDiscount,
                                                totalAmount,
                                                settledAmount,
                                                billComment,
                                                billDate,
                                                billStatus`;
                            const values = `'${holdId}',
                                            '${holdData.firmId}', 
                                            '${branchId}',
                                            '${cashier}', 
                                            'Offline',
                                            'Delivery',
                                            '${holdData.billPayType}',
                                            '${holdData.discountType}',
                                            ${holdData.discountValue},
                                            ${holdData.totalDiscount},
                                            ${holdData.subTotal},
                                            ${holdData.settledAmount},
                                            ${holdData.billComment ? `'${holdData.billComment}'` : null},
                                            STR_TO_DATE('${currentDate}','%b %d %Y'),
                                            '${holdData.billStatus}'`;

                            let sql_querry_addHoldBillInfo = `INSERT INTO hold_data (${columnData}) VALUES (${values})`;
                            connection.query(sql_querry_addHoldBillInfo, (err) => {
                                if (err) {
                                    console.error("Error inserting new bill number:", err);
                                    connection.rollback(() => {
                                        connection.release();
                                        return res.status(500).send('Database Error');
                                    });
                                } else {
                                    const billItemData = holdData.itemsData

                                    const addBillWiseItemData = [];
                                    const addItemWiseAddonData = [];

                                    billItemData.forEach((item, index) => {
                                        let uniqueId = `iwb_${Date.now() + index}_${index}`; // Unique ID generation

                                        // Construct SQL_Add_1 for the main item
                                        addBillWiseItemData.push(`('${uniqueId}', '${holdId}', '${branchId}', '${item.itemId}', ${item.qty}, '${item.unit}', ${item.itemPrice}, ${item.price}, ${item.comment ? `'${item.comment}'` : null})`);

                                        // Construct SQL_Add_2 for the addons
                                        const allAddons = item.addons ? Object.keys(item.addons) : []
                                        if (allAddons && allAddons.length) {
                                            allAddons.forEach((addonId, addonIndex) => {
                                                let iwaId = `iwa_${Date.now() + addonIndex + index}_${index}`; // Unique ID for each addon
                                                addItemWiseAddonData.push(`('${iwaId}', '${uniqueId}', '${addonId}')`);
                                            });
                                        }
                                    });

                                    let sql_query_addItems = `INSERT INTO hold_billWiseItem_data(iwbId, holdId, branchId, itemId, qty, unit, itemPrice, price, comment)
                                                              VALUES ${addBillWiseItemData.join(", ")}`;
                                    connection.query(sql_query_addItems, (err) => {
                                        if (err) {
                                            console.error("Error inserting Bill Wise Item Data:", err);
                                            connection.rollback(() => {
                                                connection.release();
                                                return res.status(500).send('Database Error');
                                            });
                                        } else {
                                            let sql_query_getHoldCount = `SELECT COUNT(*) AS holdNo FROM hold_data WHERE branchId = '${branchId}';
                                                                         ${addItemWiseAddonData.length ? `INSERT INTO hold_itemWiseAddon_data (iwaId, iwbId, addOnsId) VALUES ${addItemWiseAddonData.join(", ")}` : ''}`;
                                            connection.query(sql_query_getHoldCount, (err, count) => {
                                                if (err) {
                                                    console.error("Error inserting Bill Wise Item Data:", err);
                                                    connection.rollback(() => {
                                                        connection.release();
                                                        return res.status(500).send('Database Error');
                                                    });
                                                } else {
                                                    let holdCount = count && count[0] ? addItemWiseAddonData.length ? count[0][0].holdNo : count[0].holdNo : 0;
                                                    const customerData = holdData.customerDetails;
                                                    if (customerData && customerData.customerId && customerData.addressId) {
                                                        let sql_query_addAddressRelation = `INSERT INTO hold_billWiseCustomer_data(bwcId, holdId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                            VALUES ('${bwcId}', '${holdId}', '${customerData.customerId}', '${customerData.addressId}', ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
                                                        connection.query(sql_query_addAddressRelation, (err) => {
                                                            if (err) {
                                                                console.error("Error inserting Customer Bill Wise Data:", err);
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
                                                                        req?.io?.emit(`getHoldCount_${branchId}`, holdCount);
                                                                        return res.status(200).send("Bill Is On Hold");
                                                                    }
                                                                });
                                                            }
                                                        });
                                                    } else if (customerData && customerData.customerId && customerData.address) {
                                                        let sql_queries_chkOldAdd = `SELECT addressId, customerId FROM billing_customerAddress_data WHERE customerAddress = TRIM('${customerData.address}') AND customerLocality = '${customerData.locality}'`;
                                                        connection.query(sql_queries_chkOldAdd, (err, oldAdd) => {
                                                            if (err) {
                                                                console.error("Error inserting Customer New Address:", err);
                                                                connection.rollback(() => {
                                                                    connection.release();
                                                                    return res.status(500).send('Database Error');
                                                                });
                                                            } else {
                                                                if (oldAdd && oldAdd[0]) {
                                                                    const existAddressId = oldAdd[0].addressId;
                                                                    let sql_query_addAddressRelation = `INSERT INTO hold_billWiseCustomer_data(bwcId, holdId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                        VALUES ('${bwcId}', '${holdId}', '${customerData.customerId}', '${existAddressId}', ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
                                                                    connection.query(sql_query_addAddressRelation, (err) => {
                                                                        if (err) {
                                                                            console.error("Error inserting Customer Bill Wise Data:", err);
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
                                                                                    req?.io?.emit(`getHoldCount_${branchId}`, holdCount);
                                                                                    return res.status(200).send("Bill Is On Hold");
                                                                                }
                                                                            });
                                                                        }
                                                                    });
                                                                } else {
                                                                    let sql_querry_addNewAddress = `INSERT INTO billing_customerAddress_data(addressId, customerId, customerAddress, customerLocality)
                                                                                                    VALUES ('${newAddressId}', '${customerData.customerId}', TRIM('${customerData.address}'), ${customerData.locality ? `TRIM('${customerData.locality}')` : null})`;
                                                                    connection.query(sql_querry_addNewAddress, (err) => {
                                                                        if (err) {
                                                                            console.error("Error inserting Customer New Address:", err);
                                                                            connection.rollback(() => {
                                                                                connection.release();
                                                                                return res.status(500).send('Database Error');
                                                                            });
                                                                        } else {
                                                                            let sql_query_addAddressRelation = `INSERT INTO hold_billWiseCustomer_data(bwcId, holdId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                VALUES ('${bwcId}', '${holdId}', '${customerData.customerId}', '${newAddressId}', ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
                                                                            connection.query(sql_query_addAddressRelation, (err) => {
                                                                                if (err) {
                                                                                    console.error("Error inserting Customer Bill Wise Data:", err);
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
                                                                                            req?.io?.emit(`getHoldCount_${branchId}`, holdCount);
                                                                                            return res.status(200).send("Bill Is On Hold");
                                                                                        }
                                                                                    });
                                                                                }
                                                                            });
                                                                        }
                                                                    })
                                                                }
                                                            }
                                                        });
                                                    } else if (customerData && customerData.customerId) {
                                                        let sql_query_addAddressRelation = `INSERT INTO hold_billWiseCustomer_data(bwcId, holdId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                            VALUES ('${bwcId}', '${holdId}', '${customerData.customerId}', ${customerData.addressId ? `'${customerData.addressId}'` : null}, ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
                                                        connection.query(sql_query_addAddressRelation, (err) => {
                                                            if (err) {
                                                                console.error("Error inserting Customer Bill Wise Data:", err);
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
                                                                        req?.io?.emit(`getHoldCount_${branchId}`, holdCount);
                                                                        return res.status(200).send("Bill Is On Hold");
                                                                    }
                                                                });
                                                            }
                                                        });
                                                    } else {
                                                        if (customerData && (customerData.customerName || customerData.mobileNo)) {
                                                            let sql_querry_getExistCustomer = `SELECT customerId, customerMobileNumber FROM billing_customer_data WHERE customerMobileNumber = '${customerData.mobileNo}'`;
                                                            connection.query(sql_querry_getExistCustomer, (err, num) => {
                                                                if (err) {
                                                                    console.error("Error Get Existing Customer Data:", err);
                                                                    connection.rollback(() => {
                                                                        connection.release();
                                                                        return res.status(500).send('Database Error');
                                                                    });
                                                                } else {
                                                                    const existCustomerId = num && num[0] ? num[0].customerId : null;
                                                                    if (existCustomerId && customerData.address) {
                                                                        let sql_queries_chkOldAdd = `SELECT addressId, customerId FROM billing_customerAddress_data WHERE customerAddress = TRIM('${customerData.address}') AND customerLocality = '${customerData.locality}'`;
                                                                        connection.query(sql_queries_chkOldAdd, (err, oldAdd) => {
                                                                            if (err) {
                                                                                console.error("Error inserting Customer New Address:", err);
                                                                                connection.rollback(() => {
                                                                                    connection.release();
                                                                                    return res.status(500).send('Database Error');
                                                                                });
                                                                            } else {
                                                                                if (oldAdd && oldAdd[0]) {
                                                                                    const existAddressId = oldAdd[0].addressId;
                                                                                    let sql_query_addAddressRelation = `INSERT INTO hold_billWiseCustomer_data(bwcId, holdId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                        VALUES ('${bwcId}', '${holdId}', '${existCustomerId}', '${existAddressId}', ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
                                                                                    connection.query(sql_query_addAddressRelation, (err) => {
                                                                                        if (err) {
                                                                                            console.error("Error inserting Customer Bill Wise Data:", err);
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
                                                                                                    req?.io?.emit(`getHoldCount_${branchId}`, holdCount);
                                                                                                    return res.status(200).send("Bill Is On Hold");
                                                                                                }
                                                                                            });
                                                                                        }
                                                                                    });
                                                                                } else {
                                                                                    let sql_querry_addNewAddress = `INSERT INTO billing_customerAddress_data(addressId, customerId, customerAddress, customerLocality)
                                                                                                                    VALUES ('${newAddressId}', '${existCustomerId}', TRIM('${customerData.address}'), ${customerData.locality ? `TRIM('${customerData.locality}')` : null})`;
                                                                                    connection.query(sql_querry_addNewAddress, (err) => {
                                                                                        if (err) {
                                                                                            console.error("Error inserting Customer New Address:", err);
                                                                                            connection.rollback(() => {
                                                                                                connection.release();
                                                                                                return res.status(500).send('Database Error');
                                                                                            });
                                                                                        } else {
                                                                                            let sql_query_addAddressRelation = `INSERT INTO hold_billWiseCustomer_data(bwcId, holdId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                                VALUES ('${bwcId}', '${holdId}', '${existCustomerId}', '${newAddressId}', ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
                                                                                            connection.query(sql_query_addAddressRelation, (err) => {
                                                                                                if (err) {
                                                                                                    console.error("Error inserting Customer Bill Wise Data:", err);
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
                                                                                                            req?.io?.emit(`getHoldCount_${branchId}`, holdCount);
                                                                                                            return res.status(200).send("Bill Is On Hold");
                                                                                                        }
                                                                                                    });
                                                                                                }
                                                                                            });
                                                                                        }
                                                                                    })
                                                                                }
                                                                            }
                                                                        })
                                                                    } else if (customerData.address) {
                                                                        let sql_querry_addNewCustomer = `INSERT INTO billing_customer_data(customerId, customerName, customerMobileNumber, birthDate, anniversaryDate)
                                                                                                         VALUES ('${newCustometId}', ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.mobileNo ? `'${customerData.mobileNo}'` : null}, ${customerData.birthDate ? `STR_TO_DATE('${customerData.birthDate}','%b %d %Y')` : null}, ${customerData.aniversaryDate ? `STR_TO_DATE('${customerData.aniversaryDate}','%b %d %Y')` : null})`;
                                                                        connection.query(sql_querry_addNewCustomer, (err) => {
                                                                            if (err) {
                                                                                console.error("Error inserting New Customer Data:", err);
                                                                                connection.rollback(() => {
                                                                                    connection.release();
                                                                                    return res.status(500).send('Database Error');
                                                                                });
                                                                            } else {
                                                                                let sql_querry_addNewAddress = `INSERT INTO billing_customerAddress_data(addressId, customerId, customerAddress, customerLocality)
                                                                                                                VALUES ('${newAddressId}', '${newCustometId}', TRIM('${customerData.address}'), ${customerData.locality ? `TRIM('${customerData.locality}')` : null})`;
                                                                                connection.query(sql_querry_addNewAddress, (err) => {
                                                                                    if (err) {
                                                                                        console.error("Error inserting Customer New Address:", err);
                                                                                        connection.rollback(() => {
                                                                                            connection.release();
                                                                                            return res.status(500).send('Database Error');
                                                                                        });
                                                                                    } else {
                                                                                        let sql_query_addAddressRelation = `INSERT INTO hold_billWiseCustomer_data(bwcId, holdId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                            VALUES ('${bwcId}', '${holdId}', '${newCustometId}', '${newAddressId}', ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
                                                                                        connection.query(sql_query_addAddressRelation, (err) => {
                                                                                            if (err) {
                                                                                                console.error("Error inserting Customer Bill Wise Data:", err);
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
                                                                                                        req?.io?.emit(`getHoldCount_${branchId}`, holdCount);
                                                                                                        return res.status(200).send("Bill Is On Hold");
                                                                                                    }
                                                                                                });
                                                                                            }
                                                                                        });
                                                                                    }
                                                                                })
                                                                            }
                                                                        })
                                                                    } else if (existCustomerId) {
                                                                        let sql_query_addAddressRelation = `INSERT INTO hold_billWiseCustomer_data(bwcId, holdId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                            VALUES ('${bwcId}', '${holdId}', '${existCustomerId}', NULL, ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
                                                                        connection.query(sql_query_addAddressRelation, (err) => {
                                                                            if (err) {
                                                                                console.error("Error inserting Customer Bill Wise Data:", err);
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
                                                                                        req?.io?.emit(`getHoldCount_${branchId}`, holdCount);
                                                                                        return res.status(200).send("Bill Is On Hold");
                                                                                    }
                                                                                });
                                                                            }
                                                                        });
                                                                    } else if (customerData.mobileNo) {
                                                                        let sql_querry_addNewCustomer = `INSERT INTO billing_customer_data(customerId, customerName, customerMobileNumber, birthDate, anniversaryDate)
                                                                                                         VALUES ('${newCustometId}', ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.mobileNo ? `'${customerData.mobileNo}'` : null}, ${customerData.birthDate ? `STR_TO_DATE('${customerData.birthDate}','%b %d %Y')` : null}, ${customerData.aniversaryDate ? `STR_TO_DATE('${customerData.aniversaryDate}','%b %d %Y')` : null})`;
                                                                        connection.query(sql_querry_addNewCustomer, (err) => {
                                                                            if (err) {
                                                                                console.error("Error inserting New Customer Data:", err);
                                                                                connection.rollback(() => {
                                                                                    connection.release();
                                                                                    return res.status(500).send('Database Error');
                                                                                });
                                                                            } else {
                                                                                let sql_query_addAddressRelation = `INSERT INTO hold_billWiseCustomer_data(bwcId, holdId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                    VALUES ('${bwcId}', '${holdId}', '${newCustometId}', NULL, ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
                                                                                connection.query(sql_query_addAddressRelation, (err) => {
                                                                                    if (err) {
                                                                                        console.error("Error inserting Customer Bill Wise Data:", err);
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
                                                                                                req?.io?.emit(`getHoldCount_${branchId}`, holdCount);
                                                                                                return res.status(200).send("Bill Is On Hold");
                                                                                            }
                                                                                        });
                                                                                    }
                                                                                });
                                                                            }
                                                                        })
                                                                    } else {
                                                                        let sql_query_addAddressRelation = `INSERT INTO hold_billWiseCustomer_data(bwcId, holdId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                            VALUES ('${bwcId}', '${holdId}', NULL, NULL, ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
                                                                        connection.query(sql_query_addAddressRelation, (err) => {
                                                                            if (err) {
                                                                                console.error("Error inserting Customer Bill Wise Data:", err);
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
                                                                                        req?.io?.emit(`getHoldCount_${branchId}`, holdCount);
                                                                                        return res.status(200).send("Bill Is On Hold");
                                                                                    }
                                                                                });
                                                                            }
                                                                        });
                                                                    }
                                                                }
                                                            })
                                                        } else if (customerData.address || customerData.locality) {
                                                            let sql_query_addAddressRelation = `INSERT INTO hold_billWiseCustomer_data(bwcId, holdId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                VALUES ('${bwcId}', '${holdId}', NULL, NULL, ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
                                                            connection.query(sql_query_addAddressRelation, (err) => {
                                                                if (err) {
                                                                    console.error("Error inserting Customer Bill Wise Data:", err);
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
                                                                            req?.io?.emit(`getHoldCount_${branchId}`, holdCount);
                                                                            return res.status(200).send("Bill Is On Hold");
                                                                        }
                                                                    });
                                                                }
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
                                                                    req?.io?.emit(`getHoldCount_${branchId}`, holdCount);
                                                                    return res.status(200).send("Bill Is On Hold");
                                                                }
                                                            });
                                                        }
                                                    }
                                                }
                                            })
                                        }
                                    });
                                }
                            });
                        }
                    } else {
                        connection.rollback(() => {
                            connection.release();
                            return res.status(404).send('Please Login First....!');
                        });
                    }
                }
            });
        } catch (error) {
            console.error('An error occurred', error);
            connection.rollback(() => {
                connection.release();
                return res.status(500).json('Internal Server Error');
            })
        }
    });
}

// Hold Discard API

const discardHoldData = (req, res) => {
    try {
        let token;
        token = req.headers ? req.headers.authorization.split(" ")[1] : null;
        if (token) {
            const decoded = jwt.verify(token, process.env.JWT_SECRET);
            const branchId = decoded.id.branchId;
            const holdId = req.query.holdId;
            if (!holdId) {
                return res.status(404).send('holdId Not Found....!');
            } else {
                let sql_query_discardData = `DELETE FROM hold_data WHERE holdId = '${holdId}';
                                             DELETE FROM hold_billWiseItem_data WHERE holdId = '${holdId}';
                                             DELETE FROM hold_billWiseCustomer_data WHERE holdId = '${holdId}'`;
                pool.query(sql_query_discardData, (err, data) => {
                    if (err) {
                        console.error("An error occurred in SQL Queery", err);
                        return res.status(500).send('Database Error');
                    } else {
                        let sql_query_getHoldNumber = `SELECT COUNT(*) AS holdNo FROM hold_data WHERE branchId = '${branchId}'`;
                        pool.query(sql_query_getHoldNumber, (err, data) => {
                            if (err) {
                                console.error("An error occurred in SQL Queery", err);
                                return res.status(500).send('Database Error');
                            } else {
                                const holdCount = data && data[0] ? data[0].holdNo : 0;
                                req?.io?.emit(`getHoldCount_${branchId}`, holdCount);
                                return res.status(200).send('Discard Successfully');
                            }
                        })
                    }
                })
            }
        } else {
            return res.status(400).send('Please Login First....!');
        }
    } catch (error) {
        console.error('An error occurred', error);
        return res.status(500).json('Internal Server Error');
    }
}

module.exports = {
    getHoldBillData,
    getHoldBillDataById,
    addPickUpHoldBillData,
    addDeliveryHoldBillData,
    discardHoldData,
    getHoldCount
}

