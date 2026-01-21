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

// Get Bill Category Function By First Word

function getCategory(input) {
    switch (input.toUpperCase()) {  // Ensure input is case-insensitive
        case 'P':
            return 'Pick Up';
        case 'D':
            return 'Delivery';
        case 'R':
            return 'Dine In';
        default:
            return null;  // Default case if input doesn't match any cases
    }
}

// Get Billing Statics Data

const getBillingStaticsData = (req, res) => {
    try {
        let token;
        token = req.headers ? req.headers.authorization.split(" ")[1] : null;
        if (token) {
            const decoded = jwt.verify(token, process.env.JWT_SECRET);
            const branchId = decoded.id.branchId;
            const startDate = (req.query.startDate ? req.query.startDate : '').slice(4, 15);
            const endDate = (req.query.endDate ? req.query.endDate : '').slice(4, 15);
            const currentDate = getCurrentDate();
            let sql_queries_getStatics = `-- Pick Up
                                          SELECT
                                              COALESCE(SUM(CASE WHEN billPayType = 'cash' THEN settledAmount ELSE 0 END), 0) AS cashAmt,
                                              COALESCE(SUM(CASE WHEN billPayType = 'due' THEN settledAmount ELSE 0 END), 0) AS dueAmt,
                                              COALESCE(SUM(CASE WHEN billPayType = 'online' THEN settledAmount ELSE 0 END), 0) AS onlineAmt,
                                              COALESCE(SUM(CASE WHEN billPayType = 'complimentary' THEN settledAmount ELSE 0 END), 0) AS complimentaryAmt,
                                              COALESCE(SUM(CASE WHEN billPayType = 'cancel' THEN settledAmount ELSE 0 END), 0) AS cancleAmt,
                                              COALESCE(SUM(totalDiscount),0) AS discountAmt
                                          FROM 
                                              billing_data
                                          WHERE billType = 'Pick Up' AND branchId = '${branchId}' AND billDate BETWEEN STR_TO_DATE('${startDate ? startDate : currentDate}', '%b %d %Y') AND STR_TO_DATE('${endDate ? endDate : currentDate}', '%b %d %Y');
                                          -- Delivery
                                          SELECT
                                             COALESCE(SUM(CASE WHEN billPayType = 'cash' THEN settledAmount ELSE 0 END), 0) AS cashAmt,
                                             COALESCE(SUM(CASE WHEN billPayType = 'due' THEN settledAmount ELSE 0 END), 0) AS dueAmt,
                                             COALESCE(SUM(CASE WHEN billPayType = 'online' THEN settledAmount ELSE 0 END), 0) AS onlineAmt,
                                             COALESCE(SUM(CASE WHEN billPayType = 'complimentary' THEN settledAmount ELSE 0 END), 0) AS complimentaryAmt,
                                             COALESCE(SUM(CASE WHEN billPayType = 'cancel' THEN settledAmount ELSE 0 END), 0) AS cancleAmt,
                                             COALESCE(SUM(totalDiscount),0) AS discountAmt
                                          FROM 
                                             billing_data
                                          WHERE billType = 'Delivery' AND branchId = '${branchId}' AND billDate BETWEEN STR_TO_DATE('${startDate ? startDate : currentDate}', '%b %d %Y') AND STR_TO_DATE('${endDate ? endDate : currentDate}', '%b %d %Y');
                                          -- Dine In
                                          SELECT
                                              COALESCE(SUM(CASE WHEN billPayType = 'cash' AND billStatus IN ('print','complete') THEN settledAmount ELSE 0 END), 0) AS cashAmt,
                                              COALESCE(SUM(CASE WHEN billPayType = 'due' THEN settledAmount ELSE 0 END), 0) AS dueAmt,
                                              COALESCE(SUM(CASE WHEN billPayType = 'online' THEN settledAmount ELSE 0 END), 0) AS onlineAmt,
                                              COALESCE(SUM(CASE WHEN billPayType = 'complimentary' THEN settledAmount ELSE 0 END), 0) AS complimentaryAmt,
                                              COALESCE(SUM(CASE WHEN billPayType = 'cancel' THEN settledAmount ELSE 0 END), 0) AS cancleAmt,
                                              COALESCE(SUM(totalDiscount),0) AS discountAmt
                                          FROM billing_data
                                          WHERE billType = 'Dine In' AND billDate BETWEEN STR_TO_DATE('${startDate ? startDate : currentDate}', '%b %d %Y') AND STR_TO_DATE('${endDate ? endDate : currentDate}', '%b %d %Y');`;
            pool.query(sql_queries_getStatics, (err, data) => {
                if (err) {
                    console.error("An error occurred in SQL Queery", err);
                    return res.status(500).send('Database Error');
                } else {
                    const json = {
                        pickUp: data[0][0],
                        delivery: data[1][0],
                        dineIn: data[2][0]
                    }
                    return res.status(200).send(json);
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

// Get Live View Data

const getLiveViewByCategoryId = (req, res) => {
    try {
        let token;
        token = req.headers ? req.headers.authorization.split(" ")[1] : null;
        if (token) {
            const decoded = jwt.verify(token, process.env.JWT_SECRET);
            const branchId = decoded.id.branchId;
            const currentDate = getCurrentDate();
            const page = req.query.page;
            const numPerPage = req.query.numPerPage;
            const skip = (page - 1) * numPerPage;
            const limit = skip + ',' + numPerPage;
            const searchWord = req.query.searchWord ? req.query.searchWord : '';
            const billCategory = req.query.billCategory ? req.query.billCategory : null;
            if (billCategory) {
                sql_query_chkBillExist = `SELECT bd.billId, bd.billType FROM billing_data AS bd
                                          LEFT JOIN billing_token_data AS btd ON btd.billId = bd.billId
                                          WHERE
                                              CONCAT(
                                                  CASE bd.billType
                                                      WHEN 'Pick Up' THEN 'P'
                                                      WHEN 'Delivery' THEN 'D'
                                                      WHEN 'Dine In' THEN 'R'
                                                      ELSE ''
                                                  END,
                                                  btd.tokenNo
                                              ) LIKE '%` + searchWord + `%'
                                              AND bd.billType = '${billCategory}' AND bd.branchId = '${branchId}' AND bd.billDate = STR_TO_DATE('${currentDate}', '%b %d %Y')
                                              AND (bd.billType != 'Dine In' OR bd.billStatus IN ('print', 'complete', 'Cancel'))
                                          ORDER BY bd.billCreationDate DESC
                                          LIMIT ${limit}`;
            } else {
                sql_query_chkBillExist = `SELECT bd.billId, bd.billType FROM billing_data AS bd
                                          LEFT JOIN billing_token_data AS btd ON btd.billId = bd.billId
                                          WHERE
                                              CONCAT(
                                                  CASE bd.billType
                                                      WHEN 'Pick Up' THEN 'P'
                                                      WHEN 'Delivery' THEN 'D'
                                                      WHEN 'Dine In' THEN 'R'
                                                      ELSE ''
                                                  END,
                                                  btd.tokenNo
                                              ) LIKE '%` + searchWord + `%'
                                              AND bd.branchId = '${branchId}' AND bd.billDate = STR_TO_DATE('${currentDate}', '%b %d %Y')
                                              AND (bd.billType != 'Dine In' OR bd.billStatus IN ('print', 'complete', 'Cancel'))
                                          ORDER BY bd.billCreationDate DESC
                                          LIMIT ${limit}`;
            }
            pool.query(sql_query_chkBillExist, (err, bills) => {
                if (err) {
                    console.error("An error occurred in SQL Query", err);
                    return res.status(500).send('Database Error');
                } else {
                    if (bills && bills.length) {
                        const billDataPromises = bills.map(bill => {
                            const billId = bill.billId;
                            const billType = bill.billType;
                            let sql_query_getBillingData = `SELECT 
                                                            bd.billId AS billId, 
                                                            bd.billNumber AS billNumber,
                                                            COALESCE(bod.billNumber, CONCAT('C', bcd.billNumber), 'Not Available') AS officialBillNo,
                                                            CASE
                                                                WHEN bd.billType = 'Pick Up' THEN CONCAT('P',btd.tokenNo)
                                                                WHEN bd.billType = 'Delivery' THEN CONCAT('D',btd.tokenNo)
                                                                WHEN bd.billType = 'Dine In' THEN CONCAT('R',btd.tokenNo)
                                                                ELSE NULL
                                                            END AS tokenNo,
                                                            bwu.onlineId AS onlineId,
                                                            boud.holderName AS holderName,
                                                            boud.upiId AS upiId,
                                                            bd.firmId AS firmId, 
                                                            bd.cashier AS cashier, 
                                                            bd.menuStatus AS menuStatus, 
                                                            bd.billType AS billType, 
                                                            bd.billPayType AS billPayType, 
                                                            bd.discountType AS discountType, 
                                                            bd.discountValue AS discountValue, 
                                                            bd.totalDiscount AS totalDiscount, 
                                                            bd.totalAmount AS totalAmount, 
                                                            bd.settledAmount AS settledAmount, 
                                                            bd.billComment AS billComment, 
                                                            DATE_FORMAT(bd.billDate,'%d/%m/%Y') AS billDate,
                                                            bd.billStatus AS billStatus,
                                                            DATE_FORMAT(bd.billCreationDate,'%h:%i %p') AS billTime,
                                                            SEC_TO_TIME(
                                                                TIMESTAMPDIFF(
                                                                    SECOND,
                                                                    IF(bd.billType = 'Dine In', bwtn.printTime, bd.billCreationDate),
                                                                    NOW()
                                                                )
                                                            ) AS timeDifference
                                                        FROM 
                                                            billing_data AS bd
                                                        LEFT JOIN billing_Official_data AS bod ON bod.billId = bd.billId
                                                        LEFT JOIN billing_Complimentary_data AS bcd ON bcd.billId = bd.billId
                                                        LEFT JOIN billing_token_data AS btd ON btd.billId = bd.billId
                                                        LEFT JOIN billing_firm_data AS bfd ON bfd.firmId = bd.firmId
                                                        LEFT JOIN billing_billWiseTableNo_data AS bwtn ON bwtn.billId = bd.billId
                                                        LEFT JOIN billing_billWiseUpi_data AS bwu ON bwu.billId = bd.billId
                                                        LEFT JOIN billing_onlineUPI_data AS boud ON boud.onlineId = bwu.onlineId
                                                        WHERE bd.billId = '${billId}'`;
                            let sql_query_getBillwiseItem = `SELECT
                                                                 bwid.iwbId AS iwbId,
                                                                 bwid.itemId AS itemId,
                                                                 imd.itemName AS itemName,
                                                                 imd.itemGujaratiName AS itemGujaratiName,
                                                                 uwp.preferredName AS preferredName,
                                                                 imd.itemCode AS inputCode,
                                                                 bwid.qty AS qty,
                                                                 bwid.unit AS unit,
                                                                 bwid.itemPrice AS itemPrice,
                                                                 bwid.price AS price,
                                                                 bwid.comment AS comment
                                                             FROM
                                                                 billing_billWiseItem_data AS bwid
                                                             INNER JOIN item_menuList_data AS imd ON imd.itemId = bwid.itemId
                                                             LEFT JOIN item_unitWisePrice_data AS uwp ON uwp.itemId = bwid.itemId AND uwp.unit = bwid.unit AND uwp.menuCategoryId = '${process.env.BASE_MENU}'
                                                             WHERE bwid.billId = '${billId}'`;
                            let sql_query_getItemWiseAddons = `SELECT
                                                               iwad.iwaId AS iwaId,
                                                               iwad.iwbId AS iwbId,
                                                               iwad.addOnsId AS addOnsId,
                                                               iad.addonsName AS addonsName,
                                                               iad.addonsGujaratiName AS addonsGujaratiName,
                                                               iad.price AS addonPrice
                                                           FROM
                                                               billing_itemWiseAddon_data AS iwad
                                                           LEFT JOIN item_addons_data AS iad ON iad.addonsId = iwad.addOnsId
                                                           WHERE iwad.iwbId IN(SELECT COALESCE(bwid.iwbId, NULL) FROM billing_billWiseItem_data AS bwid WHERE bwid.billId = '${billId}')`;
                            let sql_query_getCustomerInfo = `SELECT
                                                                 bwcd.bwcId AS bwcId,
                                                                 bwcd.customerId AS customerId,
                                                                 bwcd.mobileNo AS mobileNo,
                                                                 bwcd.addressId AS addressId,
                                                                 bwcd.address AS address,
                                                                 bwcd.locality AS locality,
                                                                 bwcd.customerName AS customerName
                                                             FROM
                                                                 billing_billWiseCustomer_data AS bwcd
                                                             WHERE bwcd.billId = '${billId}'`;
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
                                                        firmId = (SELECT firmId FROM billing_data WHERE billId = '${billId}')`;
                            let sql_query_getTableData = `SELECT
                                                            bwtn.tableNo AS tableNo,
                                                            bwtn.areaId AS areaId,
                                                            bwtn.assignCaptain AS assignCaptain,
                                                            dia.areaName AS areaName,
                                                            IFNULL(CONCAT(dia.prefix, ' ', bwtn.tableNo), bwtn.tableNo) AS displayTableNo
                                                          FROM
                                                            billing_billWiseTableNo_data AS bwtn
                                                          LEFT JOIN billing_dineInArea_data dia ON dia.areaId = bwtn.areaId
                                                          WHERE billId = '${billId}'`;

                            const sql_query_getBillData = `${sql_query_getBillingData};
                                                           ${sql_query_getBillwiseItem};
                                                           ${sql_query_getFirmData};
                                                           ${sql_query_getItemWiseAddons};
                                                           ${['Pick Up', 'Delivery', 'Dine In'].includes(billType) ? sql_query_getCustomerInfo + ';' : ''}
                                                           ${billType == 'Dine In' ? sql_query_getTableData : ''}`;
                            return new Promise((resolve, reject) => {
                                pool.query(sql_query_getBillData, (err, billData) => {
                                    if (err) {
                                        console.error("An error occurred in SQL Query", err);
                                        return reject('Database Error');
                                    } else {
                                        const itemsData = billData && billData[1] ? billData[1] : [];
                                        const addonsData = billData && billData[3] ? billData[3] : [];

                                        const newItemJson = itemsData.map(item => {
                                            const itemAddons = addonsData.filter(addon => addon.iwbId === item.iwbId);
                                            return {
                                                ...item,
                                                addons: Object.fromEntries(itemAddons.map(addon => [addon.addOnsId, addon])),
                                                addonPrice: itemAddons.reduce((sum, { addonPrice }) => sum + addonPrice, 0)
                                            };
                                        });

                                        const json = {
                                            ...billData[0][0],
                                            itemData: newItemJson,
                                            firmData: billData && billData[2] ? billData[2][0] : [],
                                            ...(['Pick Up', 'Delivery', 'Dine In'].includes(billType) ? { customerDetails: billData && billData[4][0] ? billData[4][0] : '' } : ''),
                                            ...(billType === 'Dine In' ? { tableInfo: billData[5][0] } : ''),
                                            ...(['online'].includes(billData[0][0].billPayType) ? {
                                                "upiJson": {
                                                    "onlineId": billData[0][0].onlineId,
                                                    "holderName": billData[0][0].holderName,
                                                    "upiId": billData[0][0].upiId
                                                }
                                            } : '')
                                        }
                                        return resolve(json);
                                    }
                                });
                            });
                        });

                        Promise.all(billDataPromises)
                            .then(results => {
                                return res.status(200).send(results);
                            })
                            .catch(error => {
                                console.error('An error occurred', error);
                                return res.status(500).send('Internal Server Error');
                            });
                    } else {
                        return res.status(404).send('Bills Not Found');
                    }
                }
            });
        } else {
            return res.status(400).send('Please Login First....!');
        }
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).json('Internal Server Error');
    }
}

// Get Recent Bill Data

const getRecentBillData = (req, res) => {
    try {
        let token;
        token = req.headers ? req.headers.authorization.split(" ")[1] : null;
        if (token) {
            const decoded = jwt.verify(token, process.env.JWT_SECRET);
            const branchId = decoded.id.branchId;
            const billType = req.query.billType;
            const currentDate = getCurrentDate();
            if (!billType) {
                return res.status(404).send('Bill Type Not Found');
            } else {
                let sql_query_getRecentBill = `SELECT 
                                                   bd.billId AS billId, 
                                                   bd.billNumber AS billNumber,
                                                   bd.settledAmount AS totalAmount,
                                                   bd.billStatus AS billStatus,  
                                                   CASE
                                                       WHEN bd.billType = 'Pick Up' THEN CONCAT('P', btd.tokenNo)
                                                       WHEN bd.billType = 'Delivery' THEN CONCAT('D', btd.tokenNo)
                                                       WHEN bd.billType = 'Dine In' THEN CONCAT('R', btd.tokenNo)
                                                       ELSE NULL
                                                   END AS tokenNo,
                                                   CASE
                                                       WHEN bd.billType = 'Pick Up' THEN COALESCE(bwc.customerName, bwc.address, bwc.mobileNo, NULL)
                                                       WHEN bd.billType = 'Delivery' THEN COALESCE(bwc.address, bwc.customerName, bwc.mobileNo, NULL)
                                                       WHEN bd.billType = 'Dine In' THEN CONCAT('Table No. ', bwt.tableNo, ' || ', dia.areaName)
                                                       ELSE NULL
                                                   END AS address,
                                                   CASE
                                                       WHEN bd.billType = 'Pick Up' THEN
                                                           TRIM(CONCAT(
                                                               COALESCE(bwc.mobileNo, ''),
                                                               IF(bwc.mobileNo IS NOT NULL AND bwc.customerName IS NOT NULL, ' - ', ''),
                                                               COALESCE(bwc.customerName, ''),
                                                               IF((bwc.mobileNo IS NOT NULL OR bwc.customerName IS NOT NULL) AND bwc.address IS NOT NULL, ' - ', ''),
                                                               COALESCE(bwc.address, ''),
                                                               IF((bwc.mobileNo IS NOT NULL OR bwc.customerName IS NOT NULL OR bwc.address IS NOT NULL) AND bwc.locality IS NOT NULL, ' - ', ''),
                                                               COALESCE(bwc.locality, '')
                                                           ))
                                                       WHEN bd.billType = 'Delivery' THEN
                                                           TRIM(CONCAT(
                                                               COALESCE(bwc.mobileNo, ''),
                                                               IF(bwc.mobileNo IS NOT NULL AND bwc.customerName IS NOT NULL, ' - ', ''),
                                                               COALESCE(bwc.customerName, ''),
                                                               IF((bwc.mobileNo IS NOT NULL OR bwc.customerName IS NOT NULL) AND bwc.address IS NOT NULL, ' - ', ''),
                                                               COALESCE(bwc.address, ''),
                                                               IF((bwc.mobileNo IS NOT NULL OR bwc.customerName IS NOT NULL OR bwc.address IS NOT NULL) AND bwc.locality IS NOT NULL, ' - ', ''),
                                                               COALESCE(bwc.locality, '')
                                                           ))
                                                       WHEN bd.billType = 'Dine In' THEN COALESCE(bwt.assignCaptain, bwt.tableNo)
                                                       ELSE NULL
                                                   END AS info
                                               FROM billing_data AS bd
                                               LEFT JOIN billing_token_data AS btd ON btd.billId = bd.billId
                                               LEFT JOIN billing_billWiseCustomer_data AS bwc ON bwc.billId = bd.billId
                                               LEFT JOIN billing_billWiseTableNo_data AS bwt ON bwt.billId = bd.billId
                                               LEFT JOIN billing_dineInArea_data AS dia ON dia.areaId = bwt.areaId
                                               WHERE bd.branchId = '${branchId}'
                                                 AND bd.billType = '${billType}'
                                                 ${billType == 'Dine In' ? `AND bd.billStatus NOT IN ('running','print')` : ''}
                                                 AND bd.billDate = STR_TO_DATE('${currentDate}','%b %d %Y')         
                                               ORDER BY btd.tokenNo DESC;
`;
                pool.query(sql_query_getRecentBill, (err, data) => {
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
            }
        } else {
            return res.status(400).send('Please Login First....!');
        }
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).json('Internal Server Error');
    }
}

// Searh Bill Using Token No

const getBillDataByToken = (req, res) => {
    try {
        let token;
        token = req.headers ? req.headers.authorization.split(" ")[1] : null;
        if (token) {
            const decoded = jwt.verify(token, process.env.JWT_SECRET);
            const branchId = decoded.id.branchId;
            const billId = req.query.billId;
            const tokenNo = req.query.tokenNo;
            if (!tokenNo) {
                return res.status(404).send('Token Not Found');
            } else {
                const matches = tokenNo.match(/([A-Za-z]+)(\d+)/);
                if (matches) {
                    const result = [matches[1], parseInt(matches[2])];
                    const billType = getCategory(result[0]);
                    const currentDate = getCurrentDate();
                    if (billType) {
                        let sql_query_getRecentBill = `SELECT 
                                                        bd.billId AS billId, 
                                                        bd.billNumber AS billNumber,
                                                        bd.totalAmount AS totalAmount,
                                                        CASE
                                                            WHEN bd.billType = 'Pick Up' THEN CONCAT('P',btd.tokenNo)
                                                            WHEN bd.billType = 'Delivery' THEN CONCAT('D',btd.tokenNo)
                                                            WHEN bd.billType = 'Dine In' THEN CONCAT('R',btd.tokenNo)
                                                        ELSE NULL
                                                        END AS tokenNo 
                                                   FROM billing_data AS bd
                                                   LEFT JOIN billing_token_data AS btd ON btd.billId = bd.billId
                                                   WHERE bd.billType = '${billType}' AND bd.branchId = '${branchId}' AND bd.billDate = STR_TO_DATE('${currentDate}','%b %d %Y')
                                                   ORDER BY btd.tokenNo DESC`;
                        pool.query(sql_query_getRecentBill, (err, data) => {
                            if (err) {
                                console.error("An error occurred in SQL Queery", err);
                                return res.status(500).send('Database Error');
                            } else {
                                if (data && data.length) {
                                    const isBillId = data.filter((e) => {
                                        if (e.tokenNo.toUpperCase() == tokenNo.toUpperCase()) {
                                            return e.billId;
                                        } else {
                                            null
                                        }
                                    });
                                    const billId = isBillId && isBillId[0] ? isBillId[0].billId : null;
                                    if (billId) {
                                        let sql_query_getBillingData = `SELECT 
                                                                            bd.billId AS billId, 
                                                                            bd.billNumber AS billNumber,
                                                                            bd.branchId AS branchId,
                                                                            COALESCE(bod.billNumber, CONCAT('C', bcd.billNumber), 'Not Available') AS officialBillNo,
                                                                            CASE
                                                                                WHEN bd.billType = 'Pick Up' THEN CONCAT('P',btd.tokenNo)
                                                                                WHEN bd.billType = 'Delivery' THEN CONCAT('D',btd.tokenNo)
                                                                                ELSE NULL
                                                                            END AS tokenNo,
                                                                            bwu.onlineId AS onlineId,
                                                                            boud.holderName AS holderName,
                                                                            boud.upiId AS upiId,
                                                                            bd.firmId AS firmId, 
                                                                            bd.cashier AS cashier, 
                                                                            bd.menuStatus AS menuStatus, 
                                                                            bd.billType AS billType, 
                                                                            bd.billPayType AS billPayType, 
                                                                            bd.discountType AS discountType, 
                                                                            bd.discountValue AS discountValue, 
                                                                            bd.totalDiscount AS totalDiscount, 
                                                                            bd.totalAmount AS totalAmount, 
                                                                            bd.settledAmount AS settledAmount, 
                                                                            bd.billComment AS billComment, 
                                                                            DATE_FORMAT(bd.billDate,'%d/%m/%Y') AS billDate,
                                                                            bd.billStatus AS billStatus,
                                                                            DATE_FORMAT(bd.billCreationDate,'%h:%i %p') AS billTime
                                                                        FROM 
                                                                            billing_data AS bd
                                                                        LEFT JOIN billing_Official_data AS bod ON bod.billId = bd.billId
                                                                        LEFT JOIN billing_Complimentary_data AS bcd ON bcd.billId = bd.billId
                                                                        LEFT JOIN billing_token_data AS btd ON btd.billId = bd.billId
                                                                        LEFT JOIN billing_firm_data AS bfd ON bfd.firmId = bd.firmId
                                                                        LEFT JOIN billing_billWiseUpi_data AS bwu ON bwu.billId = bd.billId
                                                                        LEFT JOIN billing_onlineUPI_data AS boud ON boud.onlineId = bwu.onlineId
                                                                        WHERE bd.billId = '${billId}'`;
                                        let sql_query_getBillwiseItem = `SELECT
                                                                             bwid.iwbId AS iwbId,
                                                                             bwid.itemId AS itemId,
                                                                             imd.itemName AS itemName,
                                                                             imd.itemGujaratiName AS itemGujaratiName,
                                                                             uwp.preferredName AS preferredName,
                                                                             imd.itemCode AS inputCode,
                                                                             bwid.qty AS qty,
                                                                             bwid.unit AS unit,
                                                                             bwid.itemPrice AS itemPrice,
                                                                             bwid.price AS price,
                                                                             bwid.comment AS comment
                                                                         FROM
                                                                             billing_billWiseItem_data AS bwid
                                                                         INNER JOIN item_menuList_data AS imd ON imd.itemId = bwid.itemId
                                                                         LEFT JOIN item_unitWisePrice_data AS uwp ON uwp.itemId = bwid.itemId AND uwp.unit = bwid.unit AND uwp.menuCategoryId = '${process.env.BASE_MENU}'
                                                                         WHERE bwid.billId = '${billId}'`;
                                        let sql_query_getItemWiseAddons = `SELECT
                                                                               iwad.iwaId AS iwaId,
                                                                               iwad.iwbId AS iwbId,
                                                                               iwad.addOnsId AS addOnsId,
                                                                               iad.addonsName AS addonsName,
                                                                               iad.addonsGujaratiName AS addonsGujaratiName,
                                                                               iad.price AS addonPrice
                                                                           FROM
                                                                               billing_itemWiseAddon_data AS iwad
                                                                           LEFT JOIN item_addons_data AS iad ON iad.addonsId = iwad.addOnsId
                                                                           WHERE iwad.iwbId IN(SELECT COALESCE(bwid.iwbId, NULL) FROM billing_billWiseItem_data AS bwid WHERE bwid.billId = '${billId}')`;
                                        let sql_query_getCustomerInfo = `SELECT
                                                                             bwcd.bwcId AS bwcId,
                                                                             bwcd.customerId AS customerId,
                                                                             bwcd.mobileNo AS mobileNo,
                                                                             bwcd.addressId AS addressId,
                                                                             bwcd.address AS address,
                                                                             bwcd.locality AS locality,
                                                                             bwcd.customerName AS customerName
                                                                         FROM
                                                                             billing_billWiseCustomer_data AS bwcd
                                                                         WHERE bwcd.billId = '${billId}'`;
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
                                                                        firmId = (SELECT firmId FROM billing_data WHERE billId = '${billId}')`;
                                        let sql_query_getTableData = `SELECT
                                                                        bwtn.tableNo AS tableNo,
                                                                        bwtn.areaId AS areaId,
                                                                        bwtn.assignCaptain AS assignCaptain,
                                                                        dia.areaName AS areaName,
                                                                        IFNULL(CONCAT(dia.prefix, ' ', bwtn.tableNo), bwtn.tableNo) AS displayTableNo
                                                                      FROM
                                                                        billing_billWiseTableNo_data AS bwtn
                                                                      LEFT JOIN billing_dineInArea_data dia ON dia.areaId = bwtn.areaId
                                                                      WHERE billId = '${billId}'`;
                                        const sql_query_getBillData = `${sql_query_getBillingData};
                                                                       ${sql_query_getBillwiseItem};
                                                                       ${sql_query_getFirmData};
                                                                       ${sql_query_getItemWiseAddons};
                                                                       ${['Pick Up', 'Delivery', 'Dine In'].includes(billType) ? sql_query_getCustomerInfo + ';' : ''}
                                                                       ${billType == 'Dine In' ? sql_query_getTableData : ''}`;
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
                                                        addonPrice: itemAddons.reduce((sum, { addonPrice }) => sum + addonPrice, 0)
                                                    };
                                                });

                                                const json = {
                                                    ...billData[0][0],
                                                    itemData: newItemJson,
                                                    firmData: billData && billData[2] ? billData[2][0] : [],
                                                    ...(['Pick Up', 'Delivery', 'Dine In'].includes(billType) ? { customerDetails: billData && billData[4][0] ? billData[4][0] : '' } : ''),
                                                    ...(billType === 'Dine In' ? { tableInfo: billData[5][0] } : ''),
                                                    ...(['online'].includes(billData[0][0].billPayType) ? {
                                                        "upiJson": {
                                                            "onlineId": billData[0][0].onlineId,
                                                            "holderName": billData[0][0].holderName,
                                                            "upiId": billData[0][0].upiId
                                                        }
                                                    } : '')
                                                }
                                                return res.status(200).send(json);
                                            }
                                        })
                                    } else {
                                        return res.status(404).send('Token Number Not Found');
                                    }
                                } else {
                                    return res.status(404).send('No Data Found');
                                }
                            }
                        })
                    } else {
                        return res.status(404).send('Token Bill Type Not Found');
                    }
                } else {
                    return res.status(400).send('Token Format is Incorrect');
                }
            }
        } else {
            return res.status(400).send('Please Login First....!');
        }
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).json('Internal Server Error');
    }
}

// Fill Bill Data By Id

const getBillDataById = (req, res) => {
    try {
        let token;
        token = req.headers ? req.headers.authorization.split(" ")[1] : null;
        if (token) {
            const decoded = jwt.verify(token, process.env.JWT_SECRET);
            const branchId = decoded.id.branchId;
            const billId = req.query.billId;
            if (!billId) {
                return res.status(404).send('billId Not Found');
            } else {
                let sql_query_chkBillExist = `SELECT billId, billType, billPayType FROM billing_data WHERE billId = '${billId}' AND branchId = '${branchId}'`;
                pool.query(sql_query_chkBillExist, (err, bill) => {
                    if (err) {
                        console.error("An error occurred in SQL Queery", err);
                        return res.status(500).send('Database Error');
                    } else {
                        if (bill && bill.length) {
                            const billType = bill[0].billType;
                            const billPayType = bill[0].billPayType;
                            let sql_query_getBillingData = `SELECT 
                                                            bd.billId AS billId, 
                                                            bd.billNumber AS billNumber,
                                                            bd.branchId AS branchId,
                                                            COALESCE(bod.billNumber, CONCAT('C', bcd.billNumber), 'Not Available') AS officialBillNo,
                                                            CASE
                                                                WHEN bd.billType = 'Pick Up' THEN CONCAT('P',btd.tokenNo)
                                                                WHEN bd.billType = 'Delivery' THEN CONCAT('D',btd.tokenNo)
                                                                WHEN bd.billType = 'Dine In' THEN CONCAT('R',btd.tokenNo)
                                                                ELSE NULL
                                                            END AS tokenNo,
                                                            bwu.onlineId AS onlineId,
                                                            boud.holderName AS holderName,
                                                            boud.upiId AS upiId,
                                                            bd.firmId AS firmId, 
                                                            bd.cashier AS cashier, 
                                                            bd.menuStatus AS menuStatus, 
                                                            bd.billType AS billType, 
                                                            bd.billPayType AS billPayType, 
                                                            bd.discountType AS discountType, 
                                                            bd.discountValue AS discountValue, 
                                                            bd.totalDiscount AS totalDiscount, 
                                                            bd.totalAmount AS totalAmount, 
                                                            bd.settledAmount AS settledAmount, 
                                                            bd.billComment AS billComment, 
                                                            DATE_FORMAT(bd.billDate,'%d/%m/%Y') AS billDate,
                                                            bd.billStatus AS billStatus,
                                                            DATE_FORMAT(bd.billCreationDate,'%h:%i %p') AS billTime
                                                        FROM 
                                                            billing_data AS bd
                                                        LEFT JOIN billing_Official_data AS bod ON bod.billId = bd.billId
                                                        LEFT JOIN billing_Complimentary_data AS bcd ON bcd.billId = bd.billId
                                                        LEFT JOIN billing_token_data AS btd ON btd.billId = bd.billId
                                                        LEFT JOIN billing_firm_data AS bfd ON bfd.firmId = bd.firmId
                                                        LEFT JOIN billing_billWiseUpi_data AS bwu ON bwu.billId = bd.billId
                                                        LEFT JOIN billing_onlineUPI_data AS boud ON boud.onlineId = bwu.onlineId
                                                        WHERE bd.billId = '${billId}'`;
                            let sql_query_getBillwiseItem = `SELECT
                                                                 bwid.iwbId AS iwbId,
                                                                 bwid.itemId AS itemId,
                                                                 imd.itemName AS itemName,
                                                                 imd.itemGujaratiName AS itemGujaratiName,
                                                                 uwp.preferredName AS preferredName,
                                                                 imd.itemCode AS inputCode,
                                                                 bwid.qty AS qty,
                                                                 bwid.unit AS unit,
                                                                 bwid.itemPrice AS itemPrice,
                                                                 bwid.price AS price,
                                                                 bwid.comment AS comment
                                                             FROM
                                                                 billing_billWiseItem_data AS bwid
                                                             INNER JOIN item_menuList_data AS imd ON imd.itemId = bwid.itemId
                                                             LEFT JOIN item_unitWisePrice_data AS uwp ON uwp.itemId = bwid.itemId AND uwp.unit = bwid.unit AND uwp.menuCategoryId = '${process.env.BASE_MENU}'
                                                             WHERE bwid.billId = '${billId}'`;
                            let sql_query_getItemWiseAddons = `SELECT
                                                                   iwad.iwaId AS iwaId,
                                                                   iwad.iwbId AS iwbId,
                                                                   iwad.addOnsId AS addOnsId,
                                                                   iad.addonsName AS addonsName,
                                                                   iad.addonsGujaratiName AS addonsGujaratiName,
                                                                   iad.price AS addonPrice
                                                               FROM
                                                                   billing_itemWiseAddon_data AS iwad
                                                               LEFT JOIN item_addons_data AS iad ON iad.addonsId = iwad.addOnsId
                                                               WHERE iwad.iwbId IN(SELECT COALESCE(bwid.iwbId, NULL) FROM billing_billWiseItem_data AS bwid WHERE bwid.billId = '${billId}')`;
                            let sql_query_getCustomerInfo = `SELECT
                                                             bwcd.bwcId AS bwcId,
                                                             bwcd.customerId AS customerId,
                                                             bwcd.mobileNo AS mobileNo,
                                                             bwcd.addressId AS addressId,
                                                             bwcd.address AS address,
                                                             bwcd.locality AS locality,
                                                             bwcd.customerName AS customerName
                                                         FROM
                                                             billing_billWiseCustomer_data AS bwcd
                                                         WHERE bwcd.billId = '${billId}'`;
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
                                                            firmId = (SELECT firmId FROM billing_data WHERE billId = '${billId}')`;
                            let sql_query_getTableData = `SELECT
                                                            bwtn.tableNo AS tableNo,
                                                            bwtn.areaId AS areaId,
                                                            bwtn.assignCaptain AS assignCaptain,
                                                            dia.areaName AS areaName,
                                                            IFNULL(CONCAT(dia.prefix, ' ', bwtn.tableNo), bwtn.tableNo) AS displayTableNo
                                                          FROM
                                                            billing_billWiseTableNo_data AS bwtn
                                                          LEFT JOIN billing_dineInArea_data dia ON dia.areaId = bwtn.areaId
                                                          WHERE billId = '${billId}'`;
                            let sql_querry_getSubTokens = `SELECT subTokenNumber FROM billing_subToken_data WHERE billId = '${billId}'`;
                            const sql_query_getBillData = `${sql_query_getBillingData};
                                                           ${sql_query_getBillwiseItem};
                                                           ${sql_query_getFirmData};
                                                           ${sql_query_getItemWiseAddons};
                                                           ${['Pick Up', 'Delivery', 'Dine In'].includes(billType) ? sql_query_getCustomerInfo + ';' : ''}
                                                           ${billType == 'Dine In' ? sql_query_getTableData + ';' + sql_querry_getSubTokens : ''}`;
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
                                            addonPrice: itemAddons.reduce((sum, { addonPrice }) => sum + addonPrice, 0)
                                        };
                                    });
                                    const json = {
                                        ...billData[0][0],
                                        itemData: newItemJson,
                                        firmData: billData && billData[2] ? billData[2][0] : [],
                                        ...(['Pick Up', 'Delivery', 'Dine In'].includes(billType) ? { customerDetails: billData && billData[4][0] ? billData[4][0] : '' } : ''),
                                        ...(billType === 'Dine In' ? { tableInfo: billData[5][0] } : ''),
                                        subTokens: billData && billData[5] && billData[6].length ? billData[6].map(item => item.subTokenNumber).sort((a, b) => a - b).join(", ") : null,
                                        ...(['online'].includes(billPayType) ? {
                                            "upiJson": {
                                                "onlineId": billData[0][0].onlineId,
                                                "holderName": billData[0][0].holderName,
                                                "upiId": billData[0][0].upiId
                                            }
                                        } : '')
                                    }
                                    return res.status(200).send(json);
                                }
                            })
                        } else {
                            return res.status(404).send('Bill Id Not Found');
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

const addPickUpBillData = (req, res) => {
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
                        const billData = req.body;

                        if (!billData.customerDetails || !branchId || !billData.firmId || !billData.subTotal || !billData.settledAmount || !billData.billPayType || !billData.billStatus || !billData.itemsData) {
                            connection.rollback(() => {
                                connection.release();
                                return res.status(404).send('Please Fill All The Fields..!');
                            })
                        } else {
                            const isComplimentary = billData.billPayType == 'complimentary' ? true : false;
                            const currentDateMD = `DATE_FORMAT(STR_TO_DATE('${currentDate}', '%b %d %Y'), '%m-%d')`;
                            let sql_query_getOfficialLastBillNo = `SELECT IF(COUNT(*) = 0, 0, MAX(billNumber)) AS officialLastBillNo FROM billing_Official_data bod CROSS JOIN (SELECT COALESCE(resetDate, '04-01') AS resetDate FROM billing_firm_data WHERE firmId = '${billData.firmId}' LIMIT 1) AS frm WHERE bod.firmId = '${billData.firmId}' AND (${currentDateMD} < frm.resetDate OR (${currentDateMD} >= frm.resetDate AND DATE_FORMAT(bod.billDate, '%m-%d') >= frm.resetDate AND DATE_FORMAT(bod.billCreationDate, '%m-%d') >= frm.resetDate)) FOR UPDATE`;
                            let sql_query_getComplimentaryLastBillNo = `SELECT IF(COUNT(*) = 0, 0, MAX(billNumber)) AS complimentaryBillNo FROM billing_Complimentary_data bcd CROSS JOIN (SELECT COALESCE(resetDate, '04-01') AS resetDate FROM billing_firm_data WHERE firmId = '${billData.firmId}' LIMIT 1) AS frm WHERE bcd.firmId = '${billData.firmId}' AND (${currentDateMD} < frm.resetDate OR (${currentDateMD} >= frm.resetDate AND DATE_FORMAT(bcd.billDate, '%m-%d') >= frm.resetDate AND DATE_FORMAT(bcd.billCreationDate, '%m-%d') >= frm.resetDate)) FOR UPDATE`;
                            let sql_query_getLastBillNo = `SELECT IF(COUNT(*) = 0, 0, MAX(billNumber)) AS lastBillNo FROM billing_data bd CROSS JOIN (SELECT COALESCE(resetDate, '04-01') AS resetDate FROM billing_firm_data WHERE firmId = '${billData.firmId}' LIMIT 1) AS frm WHERE bd.firmId = '${billData.firmId}' AND (${currentDateMD} < frm.resetDate OR (${currentDateMD} >= frm.resetDate AND DATE_FORMAT(bd.billDate, '%m-%d') >= frm.resetDate AND DATE_FORMAT(bd.billCreationDate, '%m-%d') >= frm.resetDate)) FOR UPDATE;
                                                           SELECT COALESCE(MAX(tokenNo),0) AS lastTokenNo FROM billing_token_data WHERE billType = '${billData.billType}' AND branchId = '${branchId}' AND billDate = STR_TO_DATE('${currentDate}','%b %d %Y') FOR UPDATE;
                                                           ${billData.isOfficial && !isComplimentary ? sql_query_getOfficialLastBillNo : isComplimentary ? sql_query_getComplimentaryLastBillNo : ''}`;
                            connection.query(sql_query_getLastBillNo, (err, result) => {
                                if (err) {
                                    console.error("Error selecting last bill and token number:", err);
                                    connection.rollback(() => {
                                        connection.release();
                                        return res.status(500).send('Database Error');
                                    });
                                } else {
                                    const lastBillNo = result && result[0] && result[0][0].lastBillNo ? result[0][0].lastBillNo : 0;
                                    const lastTokenNo = result && result[0] && result[1][0].lastTokenNo ? result[1][0].lastTokenNo : 0;
                                    const officialLastBillNo = result && result[2] && result[2][0].officialLastBillNo ? result[2][0].officialLastBillNo : result && result[2] && result[2][0].complimentaryBillNo ? result[2][0].complimentaryBillNo : 0;

                                    const nextBillNo = lastBillNo + 1;
                                    const nextOfficialBillNo = officialLastBillNo + 1;
                                    const nextTokenNo = lastTokenNo + 1;
                                    const uid1 = new Date();
                                    const billId = String("bill_" + uid1.getTime() + '_' + nextBillNo);
                                    const tokenId = String("token_" + uid1.getTime() + '_' + nextTokenNo);
                                    const bwcId = String("bwc_" + uid1.getTime() + '_' + nextTokenNo);
                                    const newCustomerId = String("customer_" + uid1.getTime());
                                    const newAddressId = String("addressId_" + uid1.getTime());
                                    const bwuId = String("bwu_" + uid1.getTime());
                                    const dabId = String("dab_" + uid1.getTime());

                                    const columnData = `billId,
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
                                    const values = `'${billId}',
                                                   '${billData.firmId}', 
                                                   '${branchId}',
                                                   '${cashier}', 
                                                   'Offline',
                                                   'Pick Up',
                                                   '${billData.billPayType}',
                                                   '${billData.discountType}',
                                                   ${billData.discountValue},
                                                   ${billData.totalDiscount},
                                                   ${billData.subTotal},
                                                   ${billData.settledAmount},
                                                   ${billData.billComment ? `'${billData.billComment}'` : null},
                                                   STR_TO_DATE('${currentDate}','%b %d %Y'),
                                                   '${billData.billStatus}'`;

                                    let sql_querry_addBillInfo = `INSERT INTO billing_data (billNumber,${columnData}) VALUES (${nextBillNo}, ${values})`;
                                    let sql_querry_addOfficialData = `INSERT INTO billing_Official_data (billNumber, ${columnData}) VALUES(${nextOfficialBillNo}, ${values})`;
                                    let sql_querry_addComplimentaryData = `INSERT INTO billing_Complimentary_data (billNumber, ${columnData}) VALUES(${nextOfficialBillNo}, ${values})`;
                                    let sql_querry_addBillData = `${sql_querry_addBillInfo};
                                                                  ${billData.isOfficial && !isComplimentary ? sql_querry_addOfficialData : isComplimentary ? sql_querry_addComplimentaryData : ''}`;
                                    connection.query(sql_querry_addBillData, (err) => {
                                        if (err) {
                                            console.error("Error inserting new bill number:", err);
                                            connection.rollback(() => {
                                                connection.release();
                                                return res.status(500).send('Database Error');
                                            });
                                        } else {
                                            let sql_query_addTokenNo = `INSERT INTO billing_token_data(tokenId, billId, branchId, tokenNo, billType, billDate)
                                                                        VALUES ('${tokenId}', '${billId}', '${branchId}', ${nextTokenNo}, '${billData.billType}', STR_TO_DATE('${currentDate}','%b %d %Y'))`;
                                            connection.query(sql_query_addTokenNo, (err) => {
                                                if (err) {
                                                    console.error("Error inserting new Token number:", err);
                                                    connection.rollback(() => {
                                                        connection.release();
                                                        return res.status(500).send('Database Error');
                                                    });
                                                } else {
                                                    const billItemData = billData.itemsData

                                                    const addBillWiseItemData = [];
                                                    const addItemWiseAddonData = [];

                                                    billItemData.forEach((item, index) => {
                                                        let uniqueId = `iwb_${Date.now() + index}_${index}`; // Unique ID generation

                                                        // Construct SQL_Add_1 for the main item
                                                        addBillWiseItemData.push(`('${uniqueId}', '${billId}', '${branchId}', '${item.itemId}', ${item.qty}, '${item.unit}', ${item.itemPrice}, ${item.price}, ${item.comment ? `'${item.comment}'` : null}, 'Pick Up', '${billData.billPayType}', '${billData.billStatus}', STR_TO_DATE('${currentDate}','%b %d %Y'))`);

                                                        // Construct SQL_Add_2 for the addons
                                                        const allAddons = item.addons ? Object.keys(item.addons) : []
                                                        if (allAddons && allAddons.length) {
                                                            allAddons.forEach((addonId, addonIndex) => {
                                                                let iwaId = `iwa_${Date.now() + addonIndex + index}_${index}`; // Unique ID for each addon
                                                                addItemWiseAddonData.push(`('${iwaId}', '${uniqueId}', '${addonId}')`);
                                                            });
                                                        }
                                                    });

                                                    let sql_query_addItems = `INSERT INTO billing_billWiseItem_data(iwbId, billId, branchId, itemId, qty, unit, itemPrice, price, comment, billType, billPayType, billStatus, billDate)
                                                                              VALUES ${addBillWiseItemData.join(", ")}`;
                                                    connection.query(sql_query_addItems, (err) => {
                                                        if (err) {
                                                            console.error("Error inserting Bill Wise Item Data:", err);
                                                            connection.rollback(() => {
                                                                connection.release();
                                                                return res.status(500).send('Database Error');
                                                            });
                                                        } else {
                                                            let sql_query_getFirmData = `SELECT firmId, firmName, gstNumber, firmAddress, pincode, firmMobileNo, otherMobileNo FROM billing_firm_data WHERE firmId = '${billData.firmId}';
                                                                                         SELECT
                                                                                           btd.tokenNo,
                                                                                           bd.billStatus,
                                                                                           bd.billId,
                                                                                           bd.settledAmount,
                                                                                           SEC_TO_TIME(
                                                                                               TIMESTAMPDIFF(
                                                                                                   SECOND,
                                                                                                   bd.billCreationDate,
                                                                                                   NOW()
                                                                                               )
                                                                                           ) AS timeDifference
                                                                                         FROM billing_token_data AS btd
                                                                                         LEFT JOIN billing_data AS bd ON bd.billId = btd.billId
                                                                                         WHERE btd.billType = 'Pick Up' AND bd.billStatus NOT IN ('complete','Cancel') AND btd.billDate = STR_TO_DATE('${currentDate}','%b %d %Y')
                                                                                         ORDER BY btd.tokenNo ASC;
                                                                    ${addItemWiseAddonData.length
                                                                    ?
                                                                    `INSERT INTO billing_itemWiseAddon_data (iwaId, iwbId, addOnsId) VALUES ${addItemWiseAddonData.join(", ")};`
                                                                    :
                                                                    ''}
                                                                    ${billData.billPayType == 'online' && billData.onlineId && billData.onlineId != 'other'
                                                                    ?
                                                                    `INSERT INTO billing_billWiseUpi_data(bwuId, onlineId, billId, amount, onlineDate)
                                                                     VALUES('${bwuId}', '${billData.onlineId}', '${billId}', '${billData.settledAmount}', STR_TO_DATE('${currentDate}','%b %d %Y'))`
                                                                    :
                                                                    billData.accountId && billData.billPayType == 'due'
                                                                        ?
                                                                        `INSERT INTO due_billAmount_data(dabId, enterBy, accountId, billId, billAmount, dueNote, dueDate)
                                                                         VALUES('${dabId}','${cashier}','${billData.accountId}','${billId}',${billData.settledAmount},${billData.dueNote ? `'${billData.dueNote}'` : null}, STR_TO_DATE('${currentDate}','%b %d %Y'))`
                                                                        :
                                                                        ''}`;
                                                            connection.query(sql_query_getFirmData, (err, firm) => {
                                                                if (err) {
                                                                    console.error("Error inserting Bill Wise Item Data:", err);
                                                                    connection.rollback(() => {
                                                                        connection.release();
                                                                        return res.status(500).send('Database Error');
                                                                    });
                                                                } else {
                                                                    const sendJson = {
                                                                        ...billData,
                                                                        firmData: firm[0][0],
                                                                        cashier: cashier,
                                                                        billNo: nextBillNo,
                                                                        officialBillNo: billData.isOfficial && !isComplimentary ? nextOfficialBillNo : isComplimentary ? 'C' + nextOfficialBillNo : 'Not Available',
                                                                        tokenNo: 'P' + nextTokenNo,
                                                                        justToken: nextTokenNo,
                                                                        billDate: new Date(currentDate).toLocaleDateString('en-GB'),
                                                                        billTime: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
                                                                    }
                                                                    const tokenList = firm && firm[1].length ? firm[1] : null;
                                                                    const customerData = billData.customerDetails;
                                                                    if (customerData && customerData.customerId && customerData.addressId) {
                                                                        let sql_query_addAddressRelation = `INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                            VALUES ('${bwcId}', '${billId}', '${customerData.customerId}', '${customerData.addressId}', ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
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
                                                                                        req?.io?.emit('getTokenList', tokenList);
                                                                                        return res.status(200).send(sendJson);
                                                                                    }
                                                                                });
                                                                            }
                                                                        });
                                                                    } else if (customerData && customerData.customerId && customerData.address?.trim()) {
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
                                                                                    let sql_query_addAddressRelation = `INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                        VALUES ('${bwcId}', '${billId}', '${customerData.customerId}', '${existAddressId}', ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
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
                                                                                                    req?.io?.emit('getTokenList', tokenList);
                                                                                                    return res.status(200).send(sendJson);
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
                                                                                            let sql_query_addAddressRelation = `INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                                VALUES ('${bwcId}', '${billId}', '${customerData.customerId}', '${newAddressId}', ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
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
                                                                                                            req?.io?.emit('getTokenList', tokenList);
                                                                                                            return res.status(200).send(sendJson);
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
                                                                        let sql_query_addAddressRelation = `INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                            VALUES ('${bwcId}', '${billId}', '${customerData.customerId}', ${customerData.addressId ? `'${customerData.addressId}'` : null}, ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
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
                                                                                        req?.io?.emit('getTokenList', tokenList);
                                                                                        return res.status(200).send(sendJson);
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
                                                                                                    let sql_query_addAddressRelation = `INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                                        VALUES ('${bwcId}', '${billId}', '${existCustomerId}', '${existAddressId}', ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
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
                                                                                                                    req?.io?.emit('getTokenList', tokenList);
                                                                                                                    return res.status(200).send(sendJson);
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
                                                                                                            let sql_query_addAddressRelation = `INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                                                VALUES ('${bwcId}', '${billId}', '${existCustomerId}', '${newAddressId}', ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
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
                                                                                                                            req?.io?.emit('getTokenList', tokenList);
                                                                                                                            return res.status(200).send(sendJson);
                                                                                                                        }
                                                                                                                    });
                                                                                                                }
                                                                                                            });
                                                                                                        }
                                                                                                    })
                                                                                                }
                                                                                            }
                                                                                        })
                                                                                    } else if (customerData.address?.trim()) {
                                                                                        let sql_querry_addNewCustomer = `INSERT INTO billing_customer_data(customerId, customerName, customerMobileNumber, birthDate, anniversaryDate)
                                                                                                                         VALUES ('${newCustomerId}', ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.mobileNo ? `'${customerData.mobileNo}'` : null}, ${customerData.birthDate ? `STR_TO_DATE('${customerData.birthDate}','%b %d %Y')` : null}, ${customerData.aniversaryDate ? `STR_TO_DATE('${customerData.aniversaryDate}','%b %d %Y')` : null})`;
                                                                                        connection.query(sql_querry_addNewCustomer, (err) => {
                                                                                            if (err) {
                                                                                                console.error("Error inserting New Customer Data:", err);
                                                                                                connection.rollback(() => {
                                                                                                    connection.release();
                                                                                                    return res.status(500).send('Database Error');
                                                                                                });
                                                                                            } else {
                                                                                                let sql_querry_addNewAddress = `INSERT INTO billing_customerAddress_data(addressId, customerId, customerAddress, customerLocality)
                                                                                                                                VALUES ('${newAddressId}', '${newCustomerId}', TRIM('${customerData.address}'), ${customerData.locality ? `TRIM('${customerData.locality}')` : null})`;
                                                                                                connection.query(sql_querry_addNewAddress, (err) => {
                                                                                                    if (err) {
                                                                                                        console.error("Error inserting Customer New Address:", err);
                                                                                                        connection.rollback(() => {
                                                                                                            connection.release();
                                                                                                            return res.status(500).send('Database Error');
                                                                                                        });
                                                                                                    } else {
                                                                                                        let sql_query_addAddressRelation = `INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                                            VALUES ('${bwcId}', '${billId}', '${newCustomerId}', '${newAddressId}', ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
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
                                                                                                                        req?.io?.emit('getTokenList', tokenList);
                                                                                                                        return res.status(200).send(sendJson);
                                                                                                                    }
                                                                                                                });
                                                                                                            }
                                                                                                        });
                                                                                                    }
                                                                                                })
                                                                                            }
                                                                                        })
                                                                                    } else if (existCustomerId) {
                                                                                        let sql_query_addAddressRelation = `INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                            VALUES ('${bwcId}', '${billId}', '${existCustomerId}', NULL, ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
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
                                                                                                        req?.io?.emit('getTokenList', tokenList);
                                                                                                        return res.status(200).send(sendJson);
                                                                                                    }
                                                                                                });
                                                                                            }
                                                                                        });
                                                                                    } else if (customerData.mobileNo) {
                                                                                        let sql_querry_addNewCustomer = `INSERT INTO billing_customer_data(customerId, customerName, customerMobileNumber, birthDate, anniversaryDate)
                                                                                                                         VALUES ('${newCustomerId}', ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.mobileNo ? `'${customerData.mobileNo}'` : null}, ${customerData.birthDate ? `STR_TO_DATE('${customerData.birthDate}','%b %d %Y')` : null}, ${customerData.aniversaryDate ? `STR_TO_DATE('${customerData.aniversaryDate}','%b %d %Y')` : null})`;
                                                                                        connection.query(sql_querry_addNewCustomer, (err) => {
                                                                                            if (err) {
                                                                                                console.error("Error inserting New Customer Data:", err);
                                                                                                connection.rollback(() => {
                                                                                                    connection.release();
                                                                                                    return res.status(500).send('Database Error');
                                                                                                });
                                                                                            } else {
                                                                                                let sql_query_addAddressRelation = `INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                                    VALUES ('${bwcId}', '${billId}', '${newCustomerId}', NULL, ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
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
                                                                                                                req?.io?.emit('getTokenList', tokenList);
                                                                                                                return res.status(200).send(sendJson);
                                                                                                            }
                                                                                                        });
                                                                                                    }
                                                                                                });
                                                                                            }
                                                                                        })
                                                                                    } else {
                                                                                        let sql_query_addAddressRelation = `INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                            VALUES ('${bwcId}', '${billId}', NULL, NULL, ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
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
                                                                                                        req?.io?.emit('getTokenList', tokenList);
                                                                                                        return res.status(200).send(sendJson);
                                                                                                    }
                                                                                                });
                                                                                            }
                                                                                        });
                                                                                    }
                                                                                }
                                                                            })
                                                                        } else if (customerData.address?.trim() || customerData.locality?.trim()) {
                                                                            let sql_query_addAddressRelation = `INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                VALUES ('${bwcId}', '${billId}', NULL, NULL, ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
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
                                                                                            req?.io?.emit('getTokenList', tokenList);
                                                                                            return res.status(200).send(sendJson);
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
                                                                                    req?.io?.emit('getTokenList', tokenList);
                                                                                    return res.status(200).send(sendJson);
                                                                                }
                                                                            });
                                                                        }
                                                                    }
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

const addDeliveryBillData = (req, res) => {
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
                        const billData = req.body;
                        if (!billData.customerDetails || !branchId || !billData.firmId || !billData.subTotal || !billData.settledAmount || !billData.billPayType || !billData.billStatus || !billData.itemsData || !billData.customerDetails.mobileNo) {
                            connection.rollback(() => {
                                connection.release();
                                return res.status(404).send('Please Fill All The Fields..!');
                            })
                        } else {
                            const isComplimentary = billData.billPayType == 'complimentary' ? true : false;
                            const currentDateMD = `DATE_FORMAT(STR_TO_DATE('${currentDate}', '%b %d %Y'), '%m-%d')`;
                            let sql_query_getOfficialLastBillNo = `SELECT IF(COUNT(*) = 0, 0, MAX(billNumber)) AS officialLastBillNo FROM billing_Official_data bod CROSS JOIN (SELECT COALESCE(resetDate, '04-01') AS resetDate FROM billing_firm_data WHERE firmId = '${billData.firmId}' LIMIT 1) AS frm WHERE bod.firmId = '${billData.firmId}' AND (${currentDateMD} < frm.resetDate OR (${currentDateMD} >= frm.resetDate AND DATE_FORMAT(bod.billDate, '%m-%d') >= frm.resetDate AND DATE_FORMAT(bod.billCreationDate, '%m-%d') >= frm.resetDate)) FOR UPDATE`;
                            let sql_query_getComplimentaryLastBillNo = `SELECT IF(COUNT(*) = 0, 0, MAX(billNumber)) AS complimentaryBillNo FROM billing_Complimentary_data bcd CROSS JOIN (SELECT COALESCE(resetDate, '04-01') AS resetDate FROM billing_firm_data WHERE firmId = '${billData.firmId}' LIMIT 1) AS frm WHERE bcd.firmId = '${billData.firmId}' AND (${currentDateMD} < frm.resetDate OR (${currentDateMD} >= frm.resetDate AND DATE_FORMAT(bcd.billDate, '%m-%d') >= frm.resetDate AND DATE_FORMAT(bcd.billCreationDate, '%m-%d') >= frm.resetDate)) FOR UPDATE`;
                            let sql_query_getLastBillNo = `SELECT IF(COUNT(*) = 0, 0, MAX(billNumber)) AS lastBillNo FROM billing_data bd CROSS JOIN (SELECT COALESCE(resetDate, '04-01') AS resetDate FROM billing_firm_data WHERE firmId = '${billData.firmId}' LIMIT 1) AS frm WHERE bd.firmId = '${billData.firmId}' AND (${currentDateMD} < frm.resetDate OR (${currentDateMD} >= frm.resetDate AND DATE_FORMAT(bd.billDate, '%m-%d') >= frm.resetDate AND DATE_FORMAT(bd.billCreationDate, '%m-%d') >= frm.resetDate)) FOR UPDATE;
                                                           SELECT COALESCE(MAX(tokenNo),0) AS lastTokenNo FROM billing_token_data WHERE billType = '${billData.billType}' AND branchId = '${branchId}' AND billDate = STR_TO_DATE('${currentDate}','%b %d %Y') FOR UPDATE;
                                                           ${billData.isOfficial && !isComplimentary ? sql_query_getOfficialLastBillNo : isComplimentary ? sql_query_getComplimentaryLastBillNo : ''}`;
                            connection.query(sql_query_getLastBillNo, (err, result) => {
                                if (err) {
                                    console.error("Error selecting last bill and token number:", err);
                                    connection.rollback(() => {
                                        connection.release();
                                        return res.status(500).send('Database Error');
                                    });
                                } else {
                                    const lastBillNo = result && result[0] && result[0][0].lastBillNo ? result[0][0].lastBillNo : 0;
                                    const lastTokenNo = result && result[0] && result[1][0].lastTokenNo ? result[1][0].lastTokenNo : 0;
                                    const officialLastBillNo = result && result[2] && result[2][0].officialLastBillNo ? result[2][0].officialLastBillNo : result && result[2] && result[2][0].complimentaryBillNo ? result[2][0].complimentaryBillNo : 0;

                                    const nextBillNo = lastBillNo + 1;
                                    const nextOfficialBillNo = officialLastBillNo + 1;
                                    const nextTokenNo = lastTokenNo + 1;
                                    const uid1 = new Date();
                                    const billId = String("bill_" + uid1.getTime() + '_' + nextBillNo);
                                    const tokenId = String("token_" + uid1.getTime() + '_' + nextTokenNo);
                                    const bwcId = String("bwc_" + uid1.getTime() + '_' + nextTokenNo);
                                    const newCustomerId = String("customer_" + uid1.getTime());
                                    const newAddressId = String("addressId_" + uid1.getTime());
                                    const bwuId = String("bwu_" + uid1.getTime());
                                    const dabId = String("dab_" + uid1.getTime());

                                    const columnData = `billId,
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
                                    const values = `'${billId}',
                                                   '${billData.firmId}', 
                                                   '${branchId}',
                                                   '${cashier}', 
                                                   'Offline',
                                                   'Delivery',
                                                   '${billData.billPayType}',
                                                   '${billData.discountType}',
                                                   ${billData.discountValue},
                                                   ${billData.totalDiscount},
                                                   ${billData.subTotal},
                                                   ${billData.settledAmount},
                                                   ${billData.billComment ? `'${billData.billComment}'` : null},
                                                   STR_TO_DATE('${currentDate}','%b %d %Y'),
                                                   '${billData.billStatus}'`;

                                    let sql_querry_addBillInfo = `INSERT INTO billing_data (billNumber,${columnData}) VALUES (${nextBillNo}, ${values})`;
                                    let sql_querry_addOfficialData = `INSERT INTO billing_Official_data (billNumber, ${columnData}) VALUES(${nextOfficialBillNo}, ${values})`;
                                    let sql_querry_addComplimentaryData = `INSERT INTO billing_Complimentary_data (billNumber, ${columnData}) VALUES(${nextOfficialBillNo}, ${values})`;
                                    let sql_querry_addBillData = `${sql_querry_addBillInfo};
                                                                  ${billData.isOfficial && !isComplimentary ? sql_querry_addOfficialData : isComplimentary ? sql_querry_addComplimentaryData : ''}`;
                                    connection.query(sql_querry_addBillData, (err) => {
                                        if (err) {
                                            console.error("Error inserting new bill number:", err);
                                            connection.rollback(() => {
                                                connection.release();
                                                return res.status(500).send('Database Error');
                                            });
                                        } else {
                                            let sql_query_addTokenNo = `INSERT INTO billing_token_data(tokenId, billId, branchId, tokenNo, billType, billDate)
                                                                        VALUES ('${tokenId}', '${billId}', '${branchId}', ${nextTokenNo}, '${billData.billType}', STR_TO_DATE('${currentDate}','%b %d %Y'))`;
                                            connection.query(sql_query_addTokenNo, (err) => {
                                                if (err) {
                                                    console.error("Error inserting new Token number:", err);
                                                    connection.rollback(() => {
                                                        connection.release();
                                                        return res.status(500).send('Database Error');
                                                    });
                                                } else {
                                                    const billItemData = billData.itemsData

                                                    const addBillWiseItemData = [];
                                                    const addItemWiseAddonData = [];

                                                    billItemData.forEach((item, index) => {
                                                        let uniqueId = `iwb_${Date.now() + index}_${index}`; // Unique ID generation

                                                        // Construct SQL_Add_1 for the main item
                                                        addBillWiseItemData.push(`('${uniqueId}', '${billId}', '${branchId}', '${item.itemId}', ${item.qty}, '${item.unit}', ${item.itemPrice}, ${item.price}, ${item.comment ? `'${item.comment}'` : null}, 'Delivery', '${billData.billPayType}', '${billData.billStatus}', STR_TO_DATE('${currentDate}','%b %d %Y'))`);

                                                        // Construct SQL_Add_2 for the addons
                                                        const allAddons = item.addons ? Object.keys(item.addons) : []
                                                        if (allAddons && allAddons.length) {
                                                            allAddons.forEach((addonId, addonIndex) => {
                                                                let iwaId = `iwa_${Date.now() + addonIndex + index}_${index}`; // Unique ID for each addon
                                                                addItemWiseAddonData.push(`('${iwaId}', '${uniqueId}', '${addonId}')`);
                                                            });
                                                        }
                                                    });
                                                    let sql_query_addItems = `INSERT INTO billing_billWiseItem_data(iwbId, billId, branchId, itemId, qty, unit, itemPrice, price, comment, billType, billPayType, billStatus, billDate)
                                                                              VALUES ${addBillWiseItemData.join(", ")}`;
                                                    connection.query(sql_query_addItems, (err) => {
                                                        if (err) {
                                                            console.error("Error inserting Bill Wise Item Data:", err);
                                                            connection.rollback(() => {
                                                                connection.release();
                                                                return res.status(500).send('Database Error');
                                                            });
                                                        } else {
                                                            let sql_query_getFirmData = `SELECT firmId, firmName, gstNumber, firmAddress, pincode, firmMobileNo, otherMobileNo FROM billing_firm_data WHERE firmId = '${billData.firmId}';
                                                                                         SELECT
                                                                                           btd.tokenNo,
                                                                                           bd.billStatus,
                                                                                           bd.billId,
                                                                                           bd.settledAmount,
                                                                                           SEC_TO_TIME(
                                                                                               TIMESTAMPDIFF(
                                                                                                   SECOND,
                                                                                                   bd.billCreationDate,
                                                                                                   NOW()
                                                                                               )
                                                                                           ) AS timeDifference
                                                                                         FROM billing_token_data AS btd
                                                                                         LEFT JOIN billing_data AS bd ON bd.billId = btd.billId
                                                                                         WHERE btd.billType = 'Pick Up' AND bd.billStatus NOT IN ('complete','Cancel') AND btd.billDate = STR_TO_DATE('${currentDate}','%b %d %Y')
                                                                                         ORDER BY btd.tokenNo ASC;
                                                                    ${addItemWiseAddonData.length
                                                                    ?
                                                                    `INSERT INTO billing_itemWiseAddon_data (iwaId, iwbId, addOnsId) VALUES ${addItemWiseAddonData.join(", ")};`
                                                                    :
                                                                    ''}
                                                                                         ${billData.billPayType == 'online' && billData.onlineId && billData.onlineId != 'other'
                                                                    ?
                                                                    `INSERT INTO billing_billWiseUpi_data(bwuId, onlineId, billId, amount, onlineDate)
                                                                     VALUES('${bwuId}', '${billData.onlineId}', '${billId}', '${billData.settledAmount}', STR_TO_DATE('${currentDate}','%b %d %Y'))`
                                                                    :
                                                                    billData.accountId && billData.billPayType == 'due'
                                                                        ?
                                                                        `INSERT INTO due_billAmount_data(dabId, enterBy, accountId, billId, billAmount, dueNote, dueDate)
                                                                         VALUES('${dabId}','${cashier}','${billData.accountId}','${billId}',${billData.settledAmount},${billData.dueNote ? `'${billData.dueNote}'` : null}, STR_TO_DATE('${currentDate}','%b %d %Y'))`
                                                                        :
                                                                        ''}`;
                                                            connection.query(sql_query_getFirmData, (err, firm) => {
                                                                if (err) {
                                                                    console.error("Error inserting Bill Wise Item Data:", err);
                                                                    connection.rollback(() => {
                                                                        connection.release();
                                                                        return res.status(500).send('Database Error');
                                                                    });
                                                                } else {
                                                                    const sendJson = {
                                                                        ...billData,
                                                                        firmData: firm[0][0],
                                                                        cashier: cashier,
                                                                        billNo: nextBillNo,
                                                                        officialBillNo: billData.isOfficial && !isComplimentary ? nextOfficialBillNo : isComplimentary ? 'C' + nextOfficialBillNo : 'Not Available',
                                                                        tokenNo: 'D' + nextTokenNo,
                                                                        justToken: nextTokenNo,
                                                                        billDate: new Date(currentDate).toLocaleDateString('en-GB'),
                                                                        billTime: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
                                                                    }
                                                                    const tokenList = firm && firm[1].length ? firm[1] : null;
                                                                    const customerData = billData.customerDetails;
                                                                    if (customerData && customerData.customerId && customerData.addressId) {
                                                                        let sql_query_addAddressRelation = `INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                            VALUES ('${bwcId}', '${billId}', '${customerData.customerId}', '${customerData.addressId}', ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
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
                                                                                        req?.io?.emit('getTokenList', tokenList);
                                                                                        return res.status(200).send(sendJson);
                                                                                    }
                                                                                });
                                                                            }
                                                                        });
                                                                    } else if (customerData && customerData.customerId && customerData.address?.trim()) {
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
                                                                                    let sql_query_addAddressRelation = `INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                        VALUES ('${bwcId}', '${billId}', '${customerData.customerId}', '${existAddressId}', ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
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
                                                                                                    req?.io?.emit('getTokenList', tokenList);
                                                                                                    return res.status(200).send(sendJson);
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
                                                                                            let sql_query_addAddressRelation = `INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                                VALUES ('${bwcId}', '${billId}', '${customerData.customerId}', '${newAddressId}', ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
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
                                                                                                            req?.io?.emit('getTokenList', tokenList);
                                                                                                            return res.status(200).send(sendJson);
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
                                                                        let sql_query_addAddressRelation = `INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                            VALUES ('${bwcId}', '${billId}', '${customerData.customerId}', ${customerData.addressId ? `'${customerData.addressId}'` : null}, ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
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
                                                                                        req?.io?.emit('getTokenList', tokenList);
                                                                                        return res.status(200).send(sendJson);
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
                                                                                                    let sql_query_addAddressRelation = `INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                                        VALUES ('${bwcId}', '${billId}', '${existCustomerId}', '${existAddressId}', ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
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
                                                                                                                    req?.io?.emit('getTokenList', tokenList);
                                                                                                                    return res.status(200).send(sendJson);
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
                                                                                                            let sql_query_addAddressRelation = `INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                                                VALUES ('${bwcId}', '${billId}', '${existCustomerId}', '${newAddressId}', ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
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
                                                                                                                            req?.io?.emit('getTokenList', tokenList);
                                                                                                                            return res.status(200).send(sendJson);
                                                                                                                        }
                                                                                                                    });
                                                                                                                }
                                                                                                            });
                                                                                                        }
                                                                                                    })
                                                                                                }
                                                                                            }
                                                                                        })
                                                                                    } else if (customerData.address?.trim()) {
                                                                                        let sql_querry_addNewCustomer = `INSERT INTO billing_customer_data(customerId, customerName, customerMobileNumber, birthDate, anniversaryDate)
                                                                                                                         VALUES ('${newCustomerId}', ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.mobileNo ? `'${customerData.mobileNo}'` : null}, ${customerData.birthDate ? `STR_TO_DATE('${customerData.birthDate}','%b %d %Y')` : null}, ${customerData.aniversaryDate ? `STR_TO_DATE('${customerData.aniversaryDate}','%b %d %Y')` : null})`;
                                                                                        connection.query(sql_querry_addNewCustomer, (err) => {
                                                                                            if (err) {
                                                                                                console.error("Error inserting New Customer Data:", err);
                                                                                                connection.rollback(() => {
                                                                                                    connection.release();
                                                                                                    return res.status(500).send('Database Error');
                                                                                                });
                                                                                            } else {
                                                                                                let sql_querry_addNewAddress = `INSERT INTO billing_customerAddress_data(addressId, customerId, customerAddress, customerLocality)
                                                                                                                                VALUES ('${newAddressId}', '${newCustomerId}', TRIM('${customerData.address}'), ${customerData.locality ? `TRIM('${customerData.locality}')` : null})`;
                                                                                                connection.query(sql_querry_addNewAddress, (err) => {
                                                                                                    if (err) {
                                                                                                        console.error("Error inserting Customer New Address:", err);
                                                                                                        connection.rollback(() => {
                                                                                                            connection.release();
                                                                                                            return res.status(500).send('Database Error');
                                                                                                        });
                                                                                                    } else {
                                                                                                        let sql_query_addAddressRelation = `INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                                            VALUES ('${bwcId}', '${billId}', '${newCustomerId}', '${newAddressId}', ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
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
                                                                                                                        req?.io?.emit('getTokenList', tokenList);
                                                                                                                        return res.status(200).send(sendJson);
                                                                                                                    }
                                                                                                                });
                                                                                                            }
                                                                                                        });
                                                                                                    }
                                                                                                })
                                                                                            }
                                                                                        })
                                                                                    } else if (existCustomerId) {
                                                                                        let sql_query_addAddressRelation = `INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                            VALUES ('${bwcId}', '${billId}', '${existCustomerId}', NULL, ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
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
                                                                                                        req?.io?.emit('getTokenList', tokenList);
                                                                                                        return res.status(200).send(sendJson);
                                                                                                    }
                                                                                                });
                                                                                            }
                                                                                        });
                                                                                    } else if (customerData.mobileNo) {
                                                                                        let sql_querry_addNewCustomer = `INSERT INTO billing_customer_data(customerId, customerName, customerMobileNumber, birthDate, anniversaryDate)
                                                                                                                         VALUES ('${newCustomerId}', ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.mobileNo ? `'${customerData.mobileNo}'` : null}, ${customerData.birthDate ? `STR_TO_DATE('${customerData.birthDate}','%b %d %Y')` : null}, ${customerData.aniversaryDate ? `STR_TO_DATE('${customerData.aniversaryDate}','%b %d %Y')` : null})`;
                                                                                        connection.query(sql_querry_addNewCustomer, (err) => {
                                                                                            if (err) {
                                                                                                console.error("Error inserting New Customer Data:", err);
                                                                                                connection.rollback(() => {
                                                                                                    connection.release();
                                                                                                    return res.status(500).send('Database Error');
                                                                                                });
                                                                                            } else {
                                                                                                let sql_query_addAddressRelation = `INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                                    VALUES ('${bwcId}', '${billId}', '${newCustomerId}', NULL, ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
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
                                                                                                                req?.io?.emit('getTokenList', tokenList);
                                                                                                                return res.status(200).send(sendJson);
                                                                                                            }
                                                                                                        });
                                                                                                    }
                                                                                                });
                                                                                            }
                                                                                        })
                                                                                    } else {
                                                                                        let sql_query_addAddressRelation = `INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                            VALUES ('${bwcId}', '${billId}', NULL, NULL, ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
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
                                                                                                        req?.io?.emit('getTokenList', tokenList);
                                                                                                        return res.status(200).send(sendJson);
                                                                                                    }
                                                                                                });
                                                                                            }
                                                                                        });
                                                                                    }
                                                                                }
                                                                            })
                                                                        } else if (customerData.address?.trim() || customerData.locality?.trim()) {
                                                                            let sql_query_addAddressRelation = `INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                VALUES ('${bwcId}', '${billId}', NULL, NULL, ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
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
                                                                                            req?.io?.emit('getTokenList', tokenList);
                                                                                            return res.status(200).send(sendJson);
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
                                                                                    req?.io?.emit('getTokenList', tokenList);
                                                                                    return res.status(200).send(sendJson);
                                                                                }
                                                                            });
                                                                        }
                                                                    }
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

// Update PickUp Bill Data

const updatePickUpBillData = (req, res) => {
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
                        const billData = req.body;
                        const isComplimentary = billData.billPayType == 'complimentary' ? true : false;
                        if (!billData.billId || !branchId || !billData.customerDetails || !billData.subTotal || !billData.settledAmount || !billData.billPayType || !billData.billStatus || !billData.itemsData) {
                            connection.rollback(() => {
                                connection.release();
                                return res.status(404).send('Please Fill All The Fields..!');
                            })
                        } else {
                            const currentDateMD = `DATE_FORMAT(STR_TO_DATE('${currentDate}', '%b %d %Y'), '%m-%d')`;
                            let sql_query_chkOfficial = `SELECT billId, billNumber FROM billing_Official_data WHERE billId = '${billData.billId}';
                                                         SELECT billId, billNumber FROM billing_Complimentary_data WHERE billId = '${billData.billId}';
                                                         SELECT IF(COUNT(*) = 0, 0, MAX(billNumber)) AS officialLastBillNo FROM billing_Official_data bod CROSS JOIN (SELECT COALESCE(resetDate, '04-01') AS resetDate FROM billing_firm_data WHERE firmId = '${billData.firmId}' LIMIT 1) AS frm WHERE bod.firmId = '${billData.firmId}' AND (${currentDateMD} < frm.resetDate OR (${currentDateMD} >= frm.resetDate AND DATE_FORMAT(bod.billDate, '%m-%d') >= frm.resetDate AND DATE_FORMAT(bod.billCreationDate, '%m-%d') >= frm.resetDate)) FOR UPDATE;
                                                         SELECT IF(COUNT(*) = 0, 0, MAX(billNumber)) AS complimentaryLastBillNo FROM billing_Complimentary_data bcd CROSS JOIN (SELECT COALESCE(resetDate, '04-01') AS resetDate FROM billing_firm_data WHERE firmId = '${billData.firmId}' LIMIT 1) AS frm WHERE bcd.firmId = '${billData.firmId}' AND (${currentDateMD} < frm.resetDate OR (${currentDateMD} >= frm.resetDate AND DATE_FORMAT(bcd.billDate, '%m-%d') >= frm.resetDate AND DATE_FORMAT(bcd.billCreationDate, '%m-%d') >= frm.resetDate)) FOR UPDATE`;
                            connection.query(sql_query_chkOfficial, (err, chkExist) => {
                                if (err) {
                                    console.error("Error check official bill exist or not:", err);
                                    connection.rollback(() => {
                                        connection.release();
                                        return res.status(500).send('Database Error');
                                    });
                                } else {
                                    const isExist = isComplimentary ? (chkExist && chkExist[1].length ? true : false) : (chkExist && chkExist[0].length ? true : false);
                                    const staticBillNumber = isComplimentary ? (chkExist && chkExist[1].length ? chkExist[1][0].billNumber : 0) : (chkExist && chkExist[0].length ? chkExist[0][0].billNumber : 0);
                                    const officialLastBillNo = chkExist && chkExist[2] ? chkExist[2][0].officialLastBillNo : 0;
                                    const complimentaryLastBillNo = chkExist && chkExist[3] ? chkExist[3][0].complimentaryLastBillNo : 0;
                                    const nextOfficialBillNo = officialLastBillNo + 1;
                                    const nextComplimentaryBillNo = complimentaryLastBillNo + 1;
                                    let sql_query_getBillInfo = `SELECT
                                                                     bd.billId AS billId,
                                                                     bd.billNumber AS billNumber,
                                                                     DATE_FORMAT(bd.billDate, '%d/%m/%Y') AS billDate,
                                                                     DATE_FORMAT(bd.billCreationDate, '%h:%i %p') AS billTime,
                                                                     btd.tokenNo AS tokenNo
                                                                 FROM
                                                                     billing_data AS bd
                                                                 LEFT JOIN billing_token_data AS btd ON btd.billId = bd.billId
                                                                 WHERE bd.billId = '${billData.billId}' AND bd.billType = 'Pick Up'`;
                                    connection.query(sql_query_getBillInfo, (err, billInfo) => {
                                        if (err) {
                                            console.error("Error inserting new bill number:", err);
                                            connection.rollback(() => {
                                                connection.release();
                                                return res.status(500).send('Database Error');
                                            });
                                        } else {
                                            if (billInfo && billInfo.length) {
                                                const billNumber = billInfo && billInfo[0] && billInfo[0].billNunber ? billInfo[0].billNumber : 0;
                                                const tokenNo = billInfo && billInfo[0] && billInfo[0].tokenNo ? billInfo[0].tokenNo : 0;
                                                const billDate = billInfo && billInfo[0] && billInfo[0].billDate ? billInfo[0].billDate : 0;
                                                const billTime = billInfo && billInfo[0] && billInfo[0].billTime ? billInfo[0].billTime : 0;
                                                const uid1 = new Date();
                                                const bwcId = String("bwc_" + uid1.getTime() + '_' + tokenNo);
                                                const newCustomerId = String("customer_" + uid1.getTime());
                                                const newAddressId = String("addressId_" + uid1.getTime());
                                                const bwuId = String("bwu_" + uid1.getTime());
                                                const dabId = String("dab_" + uid1.getTime());

                                                const columnData = `billId,
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
                                                const values = `'${billData.billId}',
                                                                '${billData.firmId}', 
                                                                '${branchId}',
                                                                '${cashier}', 
                                                                'Offline',
                                                                'Pick Up',
                                                                '${billData.billPayType}',
                                                                '${billData.discountType}',
                                                                ${billData.discountValue},
                                                                ${billData.totalDiscount},
                                                                ${billData.subTotal},
                                                                ${billData.settledAmount},
                                                                ${billData.billComment ? `'${billData.billComment}'` : null},
                                                                STR_TO_DATE('${currentDate}','%b %d %Y'),
                                                                '${billData.billStatus}'`;

                                                let updateColumnField = `cashier = '${cashier}', 
                                                                         billPayType = '${billData.billPayType}',
                                                                         discountType = '${billData.discountType}',
                                                                         discountValue = ${billData.discountValue},
                                                                         totalDiscount = ${billData.totalDiscount},
                                                                         totalAmount = ${billData.subTotal},
                                                                         settledAmount = ${billData.settledAmount},
                                                                         billComment = ${billData.billComment ? `'${billData.billComment}'` : null},
                                                                         billDate = STR_TO_DATE('${currentDate}','%b %d %Y'),
                                                                         billStatus = '${billData.billStatus}'`;

                                                let sql_querry_updateBillInfo = `UPDATE billing_data SET ${updateColumnField} WHERE billId = '${billData.billId}';
                                                                                 ${!isExist && billData.isOfficial && !isComplimentary ?
                                                        `INSERT INTO billing_Official_data (billNumber, ${columnData}) VALUES(${nextOfficialBillNo}, ${values})` :
                                                        !isExist && isComplimentary ?
                                                            `INSERT INTO billing_Complimentary_data (billNumber, ${columnData}) VALUES(${nextComplimentaryBillNo}, ${values})` :
                                                            `UPDATE billing_Official_data SET ${updateColumnField} WHERE billId = '${billData.billId}'`};
                                                         UPDATE billing_Complimentary_data SET ${updateColumnField} WHERE billId = '${billData.billId}'`;

                                                connection.query(sql_querry_updateBillInfo, (err) => {
                                                    if (err) {
                                                        console.error("Error inserting new bill number:", err);
                                                        connection.rollback(() => {
                                                            connection.release();
                                                            return res.status(500).send('Database Error');
                                                        });
                                                    } else {
                                                        let sql_query_removeOldItemData = `DELETE FROM billing_billWiseItem_data WHERE billId = '${billData.billId}';
                                                                                           DELETE FROM billing_itemWiseAddon_data WHERE iwbId IN (SELECT COALESCE(iwbId,NULL) FROM billing_billWiseItem_data WHERE billId = '${billData.billId}')`;
                                                        connection.query(sql_query_removeOldItemData, (err) => {
                                                            if (err) {
                                                                console.error("Error inserting Bill Wise Item Data:", err);
                                                                connection.rollback(() => {
                                                                    connection.release();
                                                                    return res.status(500).send('Database Error');
                                                                });
                                                            } else {
                                                                const billItemData = billData.itemsData

                                                                const addBillWiseItemData = [];
                                                                const addItemWiseAddonData = [];

                                                                billItemData.forEach((item, index) => {
                                                                    let uniqueId = `iwb_${Date.now() + index}_${index}`; // Unique ID generation

                                                                    // Construct SQL_Add_1 for the main item
                                                                    addBillWiseItemData.push(`('${uniqueId}', '${billData.billId}', '${branchId}', '${item.itemId}', ${item.qty}, '${item.unit}', ${item.itemPrice}, ${item.price}, ${item.comment ? `'${item.comment}'` : null}, 'Pick Up', '${billData.billPayType}', '${billData.billStatus}', STR_TO_DATE('${currentDate}','%b %d %Y'))`);

                                                                    // Construct SQL_Add_2 for the addons
                                                                    const allAddons = item.addons ? Object.keys(item.addons) : []
                                                                    if (allAddons && allAddons.length) {
                                                                        allAddons.forEach((addonId, addonIndex) => {
                                                                            let iwaId = `iwa_${Date.now() + addonIndex + index}_${index}`; // Unique ID for each addon
                                                                            addItemWiseAddonData.push(`('${iwaId}', '${uniqueId}', '${addonId}')`);
                                                                        });
                                                                    }
                                                                });

                                                                let sql_query_addItems = `INSERT INTO billing_billWiseItem_data(iwbId, billId, branchId, itemId, qty, unit, itemPrice, price, comment, billType, billPayType, billStatus, billDate)
                                                                                          VALUES ${addBillWiseItemData.join(", ")}`;
                                                                connection.query(sql_query_addItems, (err) => {
                                                                    if (err) {
                                                                        console.error("Error inserting Bill Wise Item Data:", err);
                                                                        connection.rollback(() => {
                                                                            connection.release();
                                                                            return res.status(500).send('Database Error');
                                                                        });
                                                                    } else {
                                                                        let sql_query_getFirmData = `SELECT firmId, firmName, gstNumber, firmAddress, pincode, firmMobileNo, otherMobileNo FROM billing_firm_data WHERE firmId = '${billData.firmId}';
                                                                                                     SELECT
                                                                                                        btd.tokenNo,
                                                                                                        bd.billStatus,
                                                                                                        bd.billId,
                                                                                                        bd.settledAmount,
                                                                                                        SEC_TO_TIME(
                                                                                                            TIMESTAMPDIFF(
                                                                                                                SECOND,
                                                                                                                bd.billCreationDate,
                                                                                                                NOW()
                                                                                                            )
                                                                                                        ) AS timeDifference
                                                                                                     FROM billing_token_data AS btd
                                                                                                     LEFT JOIN billing_data AS bd ON bd.billId = btd.billId
                                                                                                     WHERE btd.billType = 'Pick Up' AND bd.billStatus NOT IN ('complete','Cancel') AND btd.billDate = STR_TO_DATE('${currentDate}','%b %d %Y')
                                                                                                     ORDER BY btd.tokenNo ASC;
                                                                                                     DELETE FROM billing_billWiseUpi_data WHERE billId = '${billData.billId}';
                                                                                                     ${billData.accountId && billData.billPayType == 'due' ? `DELETE FROM due_billAmount_data WHERE billId = '${billData.billId};` : ''}
                                                                                ${addItemWiseAddonData.length ? `INSERT INTO billing_itemWiseAddon_data (iwaId, iwbId, addOnsId) VALUES ${addItemWiseAddonData.join(", ")};` : ''}
                                                                                ${billData.billPayType == 'online' && billData.onlineId && billData.onlineId != 'other'
                                                                                ?
                                                                                `INSERT INTO billing_billWiseUpi_data(bwuId, onlineId, billId, amount, onlineDate)
                                                                                 VALUES('${bwuId}', '${billData.onlineId}', '${billData.billId}', '${billData.settledAmount}', STR_TO_DATE('${currentDate}','%b %d %Y'))`
                                                                                :
                                                                                billData.accountId && billData.billPayType == 'due'
                                                                                    ?
                                                                                    `INSERT INTO due_billAmount_data(dabId, enterBy, accountId, billId, billAmount, dueNote, dueDate)
                                                                                     VALUES('${dabId}','${cashier}','${billData.accountId}','${billData.billId}',${billData.settledAmount},${billData.dueNote ? `'${billData.dueNote}'` : null}, STR_TO_DATE('${currentDate}','%b %d %Y'))`
                                                                                    :
                                                                                    ''}`;
                                                                        connection.query(sql_query_getFirmData, (err, firm) => {
                                                                            if (err) {
                                                                                console.error("Error inserting Bill Wise Item Data:", err);
                                                                                connection.rollback(() => {
                                                                                    connection.release();
                                                                                    return res.status(500).send('Database Error');
                                                                                });
                                                                            } else {
                                                                                const sendJson = {
                                                                                    ...billData,
                                                                                    firmData: firm[0][0],
                                                                                    cashier: cashier,
                                                                                    billNo: billNumber,
                                                                                    officialBillNo: billData.isOfficial && !isComplimentary ? (!isExist ? nextOfficialBillNo : staticBillNumber) : isComplimentary ? (!isExist ? 'C' + nextComplimentaryBillNo : 'C' + staticBillNumber) : staticBillNumber || 'Not Available',
                                                                                    tokenNo: 'P' + tokenNo,
                                                                                    justToken: tokenNo,
                                                                                    billDate: billDate,
                                                                                    billTime: billTime
                                                                                }

                                                                                const tokenList = firm && firm[1].length ? firm[1] : null;
                                                                                const customerData = billData.customerDetails;
                                                                                if (customerData && customerData.customerId && customerData.addressId) {
                                                                                    let sql_query_addAddressRelation = `DELETE FROM billing_billWiseCustomer_data WHERE billId = '${billData.billId}';
                                                                                                                        INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                        VALUES ('${bwcId}', '${billData.billId}', '${customerData.customerId}', '${customerData.addressId}', ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
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
                                                                                                    req?.io?.emit('getTokenList', tokenList);
                                                                                                    return res.status(200).send(sendJson);
                                                                                                }
                                                                                            });
                                                                                        }
                                                                                    });
                                                                                } else if (customerData && customerData.customerId && customerData.address?.trim()) {
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
                                                                                                let sql_query_addAddressRelation = `DELETE FROM billing_billWiseCustomer_data WHERE billId = '${billData.billId}';
                                                                                                                                    INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                                    VALUES ('${bwcId}', '${billData.billId}', '${customerData.customerId}', '${existAddressId}', ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
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
                                                                                                                req?.io?.emit('getTokenList', tokenList);
                                                                                                                return res.status(200).send(sendJson);
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
                                                                                                        let sql_query_addAddressRelation = `DELETE FROM billing_billWiseCustomer_data WHERE billId = '${billData.billId}';
                                                                                                                                            INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                                            VALUES ('${bwcId}', '${billData.billId}', '${customerData.customerId}', '${newAddressId}', ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
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
                                                                                                                        req?.io?.emit('getTokenList', tokenList);
                                                                                                                        return res.status(200).send(sendJson);
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
                                                                                    let sql_query_addAddressRelation = `DELETE FROM billing_billWiseCustomer_data WHERE billId = '${billData.billId}';
                                                                                                                        INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                        VALUES ('${bwcId}', '${billData.billId}', '${customerData.customerId}', ${customerData.addressId ? `'${customerData.addressId}'` : null}, ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
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
                                                                                                    req?.io?.emit('getTokenList', tokenList);
                                                                                                    return res.status(200).send(sendJson);
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
                                                                                                                let sql_query_addAddressRelation = `DELETE FROM billing_billWiseCustomer_data WHERE billId = '${billData.billId}';
                                                                                                                                                    INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                                                    VALUES ('${bwcId}', '${billData.billId}', '${existCustomerId}', '${existAddressId}', ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
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
                                                                                                                                req?.io?.emit('getTokenList', tokenList);
                                                                                                                                return res.status(200).send(sendJson);
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
                                                                                                                        let sql_query_addAddressRelation = `DELETE FROM billing_billWiseCustomer_data WHERE billId = '${billData.billId}';
                                                                                                                                                            INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                                                            VALUES ('${bwcId}', '${billData.billId}', '${existCustomerId}', '${newAddressId}', ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
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
                                                                                                                                        req?.io?.emit('getTokenList', tokenList);
                                                                                                                                        return res.status(200).send(sendJson);
                                                                                                                                    }
                                                                                                                                });
                                                                                                                            }
                                                                                                                        });
                                                                                                                    }
                                                                                                                })
                                                                                                            }
                                                                                                        }
                                                                                                    })
                                                                                                } else if (customerData.address?.trim()) {
                                                                                                    let sql_querry_addNewCustomer = `INSERT INTO billing_customer_data(customerId, customerName, customerMobileNumber, birthDate, anniversaryDate)
                                                                                                                                     VALUES ('${newCustomerId}', ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.mobileNo ? `'${customerData.mobileNo}'` : null}, ${customerData.birthDate ? `STR_TO_DATE('${customerData.birthDate}','%b %d %Y')` : null}, ${customerData.aniversaryDate ? `STR_TO_DATE('${customerData.aniversaryDate}','%b %d %Y')` : null})`;
                                                                                                    connection.query(sql_querry_addNewCustomer, (err) => {
                                                                                                        if (err) {
                                                                                                            console.error("Error inserting New Customer Data:", err);
                                                                                                            connection.rollback(() => {
                                                                                                                connection.release();
                                                                                                                return res.status(500).send('Database Error');
                                                                                                            });
                                                                                                        } else {
                                                                                                            let sql_querry_addNewAddress = `INSERT INTO billing_customerAddress_data(addressId, customerId, customerAddress, customerLocality)
                                                                                                                                            VALUES ('${newAddressId}', '${newCustomerId}', TRIM('${customerData.address}'), ${customerData.locality ? `TRIM('${customerData.locality}')` : null})`;
                                                                                                            connection.query(sql_querry_addNewAddress, (err) => {
                                                                                                                if (err) {
                                                                                                                    console.error("Error inserting Customer New Address:", err);
                                                                                                                    connection.rollback(() => {
                                                                                                                        connection.release();
                                                                                                                        return res.status(500).send('Database Error');
                                                                                                                    });
                                                                                                                } else {
                                                                                                                    let sql_query_addAddressRelation = `DELETE FROM billing_billWiseCustomer_data WHERE billId = '${billData.billId}';
                                                                                                                                                        INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                                                        VALUES ('${bwcId}', '${billData.billId}', '${newCustomerId}', '${newAddressId}', ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
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
                                                                                                                                    req?.io?.emit('getTokenList', tokenList);
                                                                                                                                    return res.status(200).send(sendJson);
                                                                                                                                }
                                                                                                                            });
                                                                                                                        }
                                                                                                                    });
                                                                                                                }
                                                                                                            })
                                                                                                        }
                                                                                                    })
                                                                                                } else if (existCustomerId) {
                                                                                                    let sql_query_addAddressRelation = `DELETE FROM billing_billWiseCustomer_data WHERE billId = '${billData.billId}';
                                                                                                                                        INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                                        VALUES ('${bwcId}', '${billData.billId}', '${existCustomerId}', NULL, ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
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
                                                                                                                    req?.io?.emit('getTokenList', tokenList);
                                                                                                                    return res.status(200).send(sendJson);
                                                                                                                }
                                                                                                            });
                                                                                                        }
                                                                                                    });
                                                                                                } else if (customerData.mobileNo) {
                                                                                                    let sql_querry_addNewCustomer = `INSERT INTO billing_customer_data(customerId, customerName, customerMobileNumber, birthDate, anniversaryDate)
                                                                                                                                     VALUES ('${newCustomerId}', ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.mobileNo ? `'${customerData.mobileNo}'` : null}, ${customerData.birthDate ? `STR_TO_DATE('${customerData.birthDate}','%b %d %Y')` : null}, ${customerData.aniversaryDate ? `STR_TO_DATE('${customerData.aniversaryDate}','%b %d %Y')` : null})`;
                                                                                                    connection.query(sql_querry_addNewCustomer, (err) => {
                                                                                                        if (err) {
                                                                                                            console.error("Error inserting New Customer Data:", err);
                                                                                                            connection.rollback(() => {
                                                                                                                connection.release();
                                                                                                                return res.status(500).send('Database Error');
                                                                                                            });
                                                                                                        } else {
                                                                                                            let sql_query_addAddressRelation = `DELETE FROM billing_billWiseCustomer_data WHERE billId = '${billData.billId}';
                                                                                                                                                INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                                                VALUES ('${bwcId}', '${billData.billId}', '${newCustomerId}', NULL, ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
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
                                                                                                                            req?.io?.emit('getTokenList', tokenList);
                                                                                                                            return res.status(200).send(sendJson);
                                                                                                                        }
                                                                                                                    });
                                                                                                                }
                                                                                                            });
                                                                                                        }
                                                                                                    })
                                                                                                } else {
                                                                                                    let sql_query_addAddressRelation = `DELETE FROM billing_billWiseCustomer_data WHERE billId = '${billData.billId}';
                                                                                                                                        INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                                        VALUES ('${bwcId}', '${billData.billId}', NULL, NULL, ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
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
                                                                                                                    req?.io?.emit('getTokenList', tokenList);
                                                                                                                    return res.status(200).send(sendJson);
                                                                                                                }
                                                                                                            });
                                                                                                        }
                                                                                                    });
                                                                                                }
                                                                                            }
                                                                                        })
                                                                                    } else if (customerData.address?.trim() || customerData.locality?.trim()) {
                                                                                        let sql_query_addAddressRelation = `DELETE FROM billing_billWiseCustomer_data WHERE billId = '${billData.billId}';
                                                                                                                            INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                            VALUES ('${bwcId}', '${billData.billId}', NULL, NULL, ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
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
                                                                                                        req?.io?.emit('getTokenList', tokenList);
                                                                                                        return res.status(200).send(sendJson);
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
                                                                                                req?.io?.emit('getTokenList', tokenList);
                                                                                                return res.status(200).send(sendJson);
                                                                                            }
                                                                                        });
                                                                                    }
                                                                                }
                                                                            }
                                                                        });
                                                                    }
                                                                });
                                                            }
                                                        });
                                                    }
                                                });
                                            } else {
                                                connection.rollback(() => {
                                                    connection.release();
                                                    return res.status(404).send('billId Not Found...!');
                                                })
                                            }
                                        }
                                    })
                                }
                            })
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

// Update Delivery Bill Data

const updateDeliveryBillData = (req, res) => {
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
                        const billData = req.body;
                        const isComplimentary = billData.billPayType == 'complimentary' ? true : false;
                        if (!billData.billId || !branchId || !billData.customerDetails || !billData.subTotal || !billData.settledAmount || !billData.billPayType || !billData.billStatus || !billData.itemsData || !billData.customerDetails.mobileNo) {
                            connection.rollback(() => {
                                connection.release();
                                return res.status(404).send('Please Fill All The Fields..!');
                            })
                        } else {
                            const currentDateMD = `DATE_FORMAT(STR_TO_DATE('${currentDate}', '%b %d %Y'), '%m-%d')`;
                            let sql_query_chkOfficial = `SELECT billId, billNumber FROM billing_Official_data WHERE billId = '${billData.billId}';
                                                         SELECT billId, billNumber FROM billing_Complimentary_data WHERE billId = '${billData.billId}';
                                                         SELECT IF(COUNT(*) = 0, 0, MAX(billNumber)) AS officialLastBillNo FROM billing_Official_data bod CROSS JOIN (SELECT COALESCE(resetDate, '04-01') AS resetDate FROM billing_firm_data WHERE firmId = '${billData.firmId}' LIMIT 1) AS frm WHERE bod.firmId = '${billData.firmId}' AND (${currentDateMD} < frm.resetDate OR (${currentDateMD} >= frm.resetDate AND DATE_FORMAT(bod.billDate, '%m-%d') >= frm.resetDate AND DATE_FORMAT(bod.billCreationDate, '%m-%d') >= frm.resetDate)) FOR UPDATE;
                                                         SELECT IF(COUNT(*) = 0, 0, MAX(billNumber)) AS complimentaryLastBillNo FROM billing_Complimentary_data bcd CROSS JOIN (SELECT COALESCE(resetDate, '04-01') AS resetDate FROM billing_firm_data WHERE firmId = '${billData.firmId}' LIMIT 1) AS frm WHERE bcd.firmId = '${billData.firmId}' AND (${currentDateMD} < frm.resetDate OR (${currentDateMD} >= frm.resetDate AND DATE_FORMAT(bcd.billDate, '%m-%d') >= frm.resetDate AND DATE_FORMAT(bcd.billCreationDate, '%m-%d') >= frm.resetDate)) FOR UPDATE`;
                            connection.query(sql_query_chkOfficial, (err, chkExist) => {
                                if (err) {
                                    console.error("Error check official bill exist or not:", err);
                                    connection.rollback(() => {
                                        connection.release();
                                        return res.status(500).send('Database Error');
                                    });
                                } else {
                                    const isExist = isComplimentary ? (chkExist && chkExist[1].length ? true : false) : (chkExist && chkExist[0].length ? true : false);
                                    const staticBillNumber = isComplimentary ? (chkExist && chkExist[1].length ? chkExist[1][0].billNumber : 0) : (chkExist && chkExist[0].length ? chkExist[0][0].billNumber : 0);
                                    const officialLastBillNo = chkExist && chkExist[2] ? chkExist[2][0].officialLastBillNo : 0;
                                    const complimentaryLastBillNo = chkExist && chkExist[3] ? chkExist[3][0].complimentaryLastBillNo : 0;
                                    const nextOfficialBillNo = officialLastBillNo + 1;
                                    const nextComplimentaryBillNo = complimentaryLastBillNo + 1;
                                    let sql_query_getBillInfo = `SELECT
                                                                     bd.billId AS billId,
                                                                     bd.billNumber AS billNumber,
                                                                     DATE_FORMAT(bd.billDate, '%d/%m/%Y') AS billDate,
                                                                     DATE_FORMAT(bd.billCreationDate, '%h:%i %p') AS billTime,
                                                                     btd.tokenNo AS tokenNo
                                                                 FROM
                                                                     billing_data AS bd
                                                                 LEFT JOIN billing_token_data AS btd ON btd.billId = bd.billId
                                                                 WHERE bd.billId = '${billData.billId}' AND bd.billType = 'Delivery'`;
                                    connection.query(sql_query_getBillInfo, (err, billInfo) => {
                                        if (err) {
                                            console.error("Error inserting new bill number:", err);
                                            connection.rollback(() => {
                                                connection.release();
                                                return res.status(500).send('Database Error');
                                            });
                                        } else {
                                            if (billInfo && billInfo.length) {
                                                const billNumber = billInfo && billInfo[0] && billInfo[0].billNunber ? billInfo[0].billNumber : 0;
                                                const tokenNo = billInfo && billInfo[0] && billInfo[0].tokenNo ? billInfo[0].tokenNo : 0;
                                                const billDate = billInfo && billInfo[0] && billInfo[0].billDate ? billInfo[0].billDate : 0;
                                                const billTime = billInfo && billInfo[0] && billInfo[0].billTime ? billInfo[0].billTime : 0;
                                                const uid1 = new Date();
                                                const bwcId = String("bwc_" + uid1.getTime() + '_' + tokenNo);
                                                const newCustomerId = String("customer_" + uid1.getTime());
                                                const newAddressId = String("addressId_" + uid1.getTime());
                                                const bwuId = String("bwu_" + uid1.getTime());
                                                const dabId = String("dab_" + uid1.getTime());

                                                const columnData = `billId,
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
                                                const values = `'${billData.billId}',
                                                                '${billData.firmId}', 
                                                                '${branchId}',
                                                                '${cashier}', 
                                                                'Offline',
                                                                'Delivery',
                                                                '${billData.billPayType}',
                                                                '${billData.discountType}',
                                                                ${billData.discountValue},
                                                                ${billData.totalDiscount},
                                                                ${billData.subTotal},
                                                                ${billData.settledAmount},
                                                                ${billData.billComment ? `'${billData.billComment}'` : null},
                                                                STR_TO_DATE('${currentDate}','%b %d %Y'),
                                                                '${billData.billStatus}'`;

                                                let updateColumnField = `cashier = '${cashier}', 
                                                                         billPayType = '${billData.billPayType}',
                                                                         discountType = '${billData.discountType}',
                                                                         discountValue = ${billData.discountValue},
                                                                         totalDiscount = ${billData.totalDiscount},
                                                                         totalAmount = ${billData.subTotal},
                                                                         settledAmount = ${billData.settledAmount},
                                                                         billComment = ${billData.billComment ? `'${billData.billComment}'` : null},
                                                                         billDate = STR_TO_DATE('${currentDate}','%b %d %Y'),
                                                                         billStatus = '${billData.billStatus}'`;

                                                let sql_querry_updateBillInfo = `UPDATE billing_data SET ${updateColumnField} WHERE billId = '${billData.billId}';
                                                                                 ${!isExist && billData.isOfficial && !isComplimentary ?
                                                        `INSERT INTO billing_Official_data (billNumber, ${columnData}) VALUES(${nextOfficialBillNo}, ${values})` :
                                                        !isExist && isComplimentary ?
                                                            `INSERT INTO billing_Complimentary_data (billNumber, ${columnData}) VALUES(${nextComplimentaryBillNo}, ${values})` :
                                                            `UPDATE billing_Official_data SET ${updateColumnField} WHERE billId = '${billData.billId}'`};
                                                         UPDATE billing_Complimentary_data SET ${updateColumnField} WHERE billId = '${billData.billId}'`;

                                                connection.query(sql_querry_updateBillInfo, (err) => {
                                                    if (err) {
                                                        console.error("Error inserting new bill number:", err);
                                                        connection.rollback(() => {
                                                            connection.release();
                                                            return res.status(500).send('Database Error');
                                                        });
                                                    } else {
                                                        let sql_query_removeOldItemData = `DELETE FROM billing_billWiseItem_data WHERE billId = '${billData.billId}';
                                                                                           DELETE FROM billing_itemWiseAddon_data WHERE iwbId IN (SELECT COALESCE(iwbId,NULL) FROM billing_billWiseItem_data WHERE billId = '${billData.billId}')`;
                                                        connection.query(sql_query_removeOldItemData, (err) => {
                                                            if (err) {
                                                                console.error("Error inserting Bill Wise Item Data:", err);
                                                                connection.rollback(() => {
                                                                    connection.release();
                                                                    return res.status(500).send('Database Error');
                                                                });
                                                            } else {
                                                                const billItemData = billData.itemsData

                                                                const addBillWiseItemData = [];
                                                                const addItemWiseAddonData = [];

                                                                billItemData.forEach((item, index) => {
                                                                    let uniqueId = `iwb_${Date.now() + index}_${index}`; // Unique ID generation

                                                                    // Construct SQL_Add_1 for the main item
                                                                    addBillWiseItemData.push(`('${uniqueId}', '${billData.billId}', '${branchId}', '${item.itemId}', ${item.qty}, '${item.unit}', ${item.itemPrice}, ${item.price}, ${item.comment ? `'${item.comment}'` : null}, 'Delivery', '${billData.billPayType}', '${billData.billStatus}', STR_TO_DATE('${currentDate}','%b %d %Y'))`);

                                                                    // Construct SQL_Add_2 for the addons
                                                                    const allAddons = item.addons ? Object.keys(item.addons) : []
                                                                    if (allAddons && allAddons.length) {
                                                                        allAddons.forEach((addonId, addonIndex) => {
                                                                            let iwaId = `iwa_${Date.now() + addonIndex + index}_${index}`; // Unique ID for each addon
                                                                            addItemWiseAddonData.push(`('${iwaId}', '${uniqueId}', '${addonId}')`);
                                                                        });
                                                                    }
                                                                });
                                                                let sql_query_addItems = `INSERT INTO billing_billWiseItem_data(iwbId, billId, branchId, itemId, qty, unit, itemPrice, price, comment, billType, billPayType, billStatus, billDate)
                                                                                          VALUES ${addBillWiseItemData.join(", ")}`;
                                                                connection.query(sql_query_addItems, (err) => {
                                                                    if (err) {
                                                                        console.error("Error inserting Bill Wise Item Data:", err);
                                                                        connection.rollback(() => {
                                                                            connection.release();
                                                                            return res.status(500).send('Database Error');
                                                                        });
                                                                    } else {
                                                                        let sql_query_getFirmData = `SELECT firmId, firmName, gstNumber, firmAddress, pincode, firmMobileNo, otherMobileNo FROM billing_firm_data WHERE firmId = '${billData.firmId}';
                                                                                                     SELECT
                                                                                                        btd.tokenNo,
                                                                                                        bd.billStatus,
                                                                                                        bd.billId,
                                                                                                        bd.settledAmount,
                                                                                                        SEC_TO_TIME(
                                                                                                            TIMESTAMPDIFF(
                                                                                                                SECOND,
                                                                                                                bd.billCreationDate,
                                                                                                                NOW()
                                                                                                            )
                                                                                                        ) AS timeDifference
                                                                                                     FROM billing_token_data AS btd
                                                                                                     LEFT JOIN billing_data AS bd ON bd.billId = btd.billId
                                                                                                     WHERE btd.billType = 'Delivery' AND bd.billStatus NOT IN ('complete','Cancel') AND btd.billDate = STR_TO_DATE('${currentDate}','%b %d %Y')
                                                                                                     ORDER BY btd.tokenNo ASC;
                                                                                                     DELETE FROM billing_billWiseUpi_data WHERE billId = '${billData.billId}';
                                                                                                     ${billData.accountId && billData.billPayType == 'due' ? `DELETE FROM due_billAmount_data WHERE billId = '${billData.billId};` : ''}
                                                                                ${addItemWiseAddonData.length ? `INSERT INTO billing_itemWiseAddon_data (iwaId, iwbId, addOnsId) VALUES ${addItemWiseAddonData.join(", ")};` : ''}
                                                                                ${billData.billPayType == 'online' && billData.onlineId && billData.onlineId != 'other'
                                                                                ?
                                                                                `INSERT INTO billing_billWiseUpi_data(bwuId, onlineId, billId, amount, onlineDate)
                                                                                 VALUES('${bwuId}', '${billData.onlineId}', '${billData.billId}', '${billData.settledAmount}', STR_TO_DATE('${currentDate}','%b %d %Y'))`
                                                                                :
                                                                                billData.accountId && billData.billPayType == 'due'
                                                                                    ?
                                                                                    `INSERT INTO due_billAmount_data(dabId, enterBy, accountId, billId, billAmount, dueNote, dueDate)
                                                                                     VALUES('${dabId}','${cashier}','${billData.accountId}','${billData.billId}',${billData.settledAmount},${billData.dueNote ? `'${billData.dueNote}'` : null}, STR_TO_DATE('${currentDate}','%b %d %Y'))`
                                                                                    :
                                                                                    ''}`;
                                                                        connection.query(sql_query_getFirmData, (err, firm) => {
                                                                            if (err) {
                                                                                console.error("Error inserting Bill Wise Item Data:", err);
                                                                                connection.rollback(() => {
                                                                                    connection.release();
                                                                                    return res.status(500).send('Database Error');
                                                                                });
                                                                            } else {
                                                                                const sendJson = {
                                                                                    ...billData,
                                                                                    firmData: firm[0][0],
                                                                                    cashier: cashier,
                                                                                    billNo: billNumber,
                                                                                    officialBillNo: billData.isOfficial && !isComplimentary ? (!isExist ? nextOfficialBillNo : staticBillNumber) : isComplimentary ? (!isExist ? 'C' + nextComplimentaryBillNo : 'C' + staticBillNumber) : staticBillNumber || 'Not Available',
                                                                                    tokenNo: 'D' + tokenNo,
                                                                                    justToken: tokenNo,
                                                                                    billDate: billDate,
                                                                                    billTime: billTime
                                                                                }
                                                                                const tokenList = firm && firm[1].length ? firm[1] : null;
                                                                                const customerData = billData.customerDetails;
                                                                                if (customerData && customerData.customerId && customerData.addressId) {
                                                                                    let sql_query_addAddressRelation = `DELETE FROM billing_billWiseCustomer_data WHERE billId = '${billData.billId}';
                                                                                                                        INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                        VALUES ('${bwcId}', '${billData.billId}', '${customerData.customerId}', '${customerData.addressId}', ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
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
                                                                                                    req?.io?.emit('getTokenList', tokenList);
                                                                                                    return res.status(200).send(sendJson);
                                                                                                }
                                                                                            });
                                                                                        }
                                                                                    });
                                                                                } else if (customerData && customerData.customerId && customerData.address?.trim()) {
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
                                                                                                let sql_query_addAddressRelation = `DELETE FROM billing_billWiseCustomer_data WHERE billId = '${billData.billId}';
                                                                                                                                    INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                                    VALUES ('${bwcId}', '${billData.billId}', '${customerData.customerId}', '${existAddressId}', ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
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
                                                                                                                req?.io?.emit('getTokenList', tokenList);
                                                                                                                return res.status(200).send(sendJson);
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
                                                                                                        let sql_query_addAddressRelation = `DELETE FROM billing_billWiseCustomer_data WHERE billId = '${billData.billId}';
                                                                                                                                            INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                                            VALUES ('${bwcId}', '${billData.billId}', '${customerData.customerId}', '${newAddressId}', ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
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
                                                                                                                        req?.io?.emit('getTokenList', tokenList);
                                                                                                                        return res.status(200).send(sendJson);
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
                                                                                    let sql_query_addAddressRelation = `DELETE FROM billing_billWiseCustomer_data WHERE billId = '${billData.billId}';
                                                                                                                        INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                        VALUES ('${bwcId}', '${billData.billId}', '${customerData.customerId}', ${customerData.addressId ? `'${customerData.addressId}'` : null}, ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
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
                                                                                                    req?.io?.emit('getTokenList', tokenList);
                                                                                                    return res.status(200).send(sendJson);
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
                                                                                                                let sql_query_addAddressRelation = `DELETE FROM billing_billWiseCustomer_data WHERE billId = '${billData.billId}';
                                                                                                                                                    INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                                                    VALUES ('${bwcId}', '${billData.billId}', '${existCustomerId}', '${existAddressId}', ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
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
                                                                                                                                req?.io?.emit('getTokenList', tokenList);
                                                                                                                                return res.status(200).send(sendJson);
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
                                                                                                                        let sql_query_addAddressRelation = `DELETE FROM billing_billWiseCustomer_data WHERE billId = '${billData.billId}';
                                                                                                                                                            INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                                                            VALUES ('${bwcId}', '${billData.billId}', '${existCustomerId}', '${newAddressId}', ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
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
                                                                                                                                        req?.io?.emit('getTokenList', tokenList);
                                                                                                                                        return res.status(200).send(sendJson);
                                                                                                                                    }
                                                                                                                                });
                                                                                                                            }
                                                                                                                        });
                                                                                                                    }
                                                                                                                })
                                                                                                            }
                                                                                                        }
                                                                                                    })
                                                                                                } else if (customerData.address?.trim()) {
                                                                                                    let sql_querry_addNewCustomer = `INSERT INTO billing_customer_data(customerId, customerName, customerMobileNumber, birthDate, anniversaryDate)
                                                                                                                                     VALUES ('${newCustomerId}', ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.mobileNo ? `'${customerData.mobileNo}'` : null}, ${customerData.birthDate ? `STR_TO_DATE('${customerData.birthDate}','%b %d %Y')` : null}, ${customerData.aniversaryDate ? `STR_TO_DATE('${customerData.aniversaryDate}','%b %d %Y')` : null})`;
                                                                                                    connection.query(sql_querry_addNewCustomer, (err) => {
                                                                                                        if (err) {
                                                                                                            console.error("Error inserting New Customer Data:", err);
                                                                                                            connection.rollback(() => {
                                                                                                                connection.release();
                                                                                                                return res.status(500).send('Database Error');
                                                                                                            });
                                                                                                        } else {
                                                                                                            let sql_querry_addNewAddress = `INSERT INTO billing_customerAddress_data(addressId, customerId, customerAddress, customerLocality)
                                                                                                                                            VALUES ('${newAddressId}', '${newCustomerId}', TRIM('${customerData.address}'), ${customerData.locality ? `TRIM('${customerData.locality}')` : null})`;
                                                                                                            connection.query(sql_querry_addNewAddress, (err) => {
                                                                                                                if (err) {
                                                                                                                    console.error("Error inserting Customer New Address:", err);
                                                                                                                    connection.rollback(() => {
                                                                                                                        connection.release();
                                                                                                                        return res.status(500).send('Database Error');
                                                                                                                    });
                                                                                                                } else {
                                                                                                                    let sql_query_addAddressRelation = `DELETE FROM billing_billWiseCustomer_data WHERE billId = '${billData.billId}';
                                                                                                                                                        INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                                                        VALUES ('${bwcId}', '${billData.billId}', '${newCustomerId}', '${newAddressId}', ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
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
                                                                                                                                    req?.io?.emit('getTokenList', tokenList);
                                                                                                                                    return res.status(200).send(sendJson);
                                                                                                                                }
                                                                                                                            });
                                                                                                                        }
                                                                                                                    });
                                                                                                                }
                                                                                                            })
                                                                                                        }
                                                                                                    })
                                                                                                } else if (existCustomerId) {
                                                                                                    let sql_query_addAddressRelation = `DELETE FROM billing_billWiseCustomer_data WHERE billId = '${billData.billId}';
                                                                                                                                        INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                                        VALUES ('${bwcId}', '${billData.billId}', '${existCustomerId}', NULL, ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
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
                                                                                                                    req?.io?.emit('getTokenList', tokenList);
                                                                                                                    return res.status(200).send(sendJson);
                                                                                                                }
                                                                                                            });
                                                                                                        }
                                                                                                    });
                                                                                                } else if (customerData.mobileNo) {
                                                                                                    let sql_querry_addNewCustomer = `INSERT INTO billing_customer_data(customerId, customerName, customerMobileNumber, birthDate, anniversaryDate)
                                                                                                                                     VALUES ('${newCustomerId}', ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.mobileNo ? `'${customerData.mobileNo}'` : null}, ${customerData.birthDate ? `STR_TO_DATE('${customerData.birthDate}','%b %d %Y')` : null}, ${customerData.aniversaryDate ? `STR_TO_DATE('${customerData.aniversaryDate}','%b %d %Y')` : null})`;
                                                                                                    connection.query(sql_querry_addNewCustomer, (err) => {
                                                                                                        if (err) {
                                                                                                            console.error("Error inserting New Customer Data:", err);
                                                                                                            connection.rollback(() => {
                                                                                                                connection.release();
                                                                                                                return res.status(500).send('Database Error');
                                                                                                            });
                                                                                                        } else {
                                                                                                            let sql_query_addAddressRelation = `DELETE FROM billing_billWiseCustomer_data WHERE billId = '${billData.billId}';
                                                                                                                                                INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                                                VALUES ('${bwcId}', '${billData.billId}', '${newCustomerId}', NULL, ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
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
                                                                                                                            req?.io?.emit('getTokenList', tokenList);
                                                                                                                            return res.status(200).send(sendJson);
                                                                                                                        }
                                                                                                                    });
                                                                                                                }
                                                                                                            });
                                                                                                        }
                                                                                                    })
                                                                                                } else {
                                                                                                    let sql_query_addAddressRelation = `DELETE FROM billing_billWiseCustomer_data WHERE billId = '${billData.billId}';
                                                                                                                                        INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                                        VALUES ('${bwcId}', '${billData.billId}', NULL, NULL, ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
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
                                                                                                                    req?.io?.emit('getTokenList', tokenList);
                                                                                                                    return res.status(200).send(sendJson);
                                                                                                                }
                                                                                                            });
                                                                                                        }
                                                                                                    });
                                                                                                }
                                                                                            }
                                                                                        })
                                                                                    } else if (customerData.address?.trim() || customerData.locality?.trim()) {
                                                                                        let sql_query_addAddressRelation = `DELETE FROM billing_billWiseCustomer_data WHERE billId = '${billData.billId}';
                                                                                                                            INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                            VALUES ('${bwcId}', '${billData.billId}', NULL, NULL, ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
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
                                                                                                        req?.io?.emit('getTokenList', tokenList);
                                                                                                        return res.status(200).send(sendJson);
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
                                                                                                req?.io?.emit('getTokenList', tokenList);
                                                                                                return res.status(200).send(sendJson);
                                                                                            }
                                                                                        });
                                                                                    }
                                                                                }
                                                                            }
                                                                        });
                                                                    }
                                                                });
                                                            }
                                                        });
                                                    }
                                                });
                                            } else {
                                                connection.rollback(() => {
                                                    connection.release();
                                                    return res.status(404).send('billId Not Found...!');
                                                })
                                            }
                                        }
                                    })
                                }
                            })
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

// Update Bill Status In Live View

const updateBillStatusById = (req, res) => {
    try {
        const billId = req.query.billId;
        const billStatus = req.query.billStatus;
        if (!billId || !billStatus) {
            return res.status(404).send('Bill Id Not Found !')
        } else {
            let sql_query_updateBillStatus = `UPDATE billing_data SET billStatus = '${billStatus}' WHERE billId = '${billId}';
                                              UPDATE billing_Official_data SET billStatus = '${billStatus}' WHERE billId = '${billId}';
                                              UPDATE billing_Complimentary_data SET billStatus = '${billStatus}' WHERE billId = '${billId}'`;
            pool.query(sql_query_updateBillStatus, (err, data) => {
                if (err) {
                    console.error("An error occurred in SQL Queery", err);
                    return res.status(500).send('Database Error');
                } else {
                    return res.status(200).send('Status Updated Succesfully');
                }
            })
        }
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).json('Internal Server Error');
    }
}

// Print Old Bills

const printBillInAdminSystem = (req, res) => {
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

                        const billId = req.query.billId;

                        let sql_query_getBillingData = `SELECT 
                                                            bd.billId AS billId, 
                                                            bd.billNumber AS billNumber,
                                                            COALESCE(bod.billNumber, CONCAT('C', bcd.billNumber), 'Not Available') AS officialBillNo,
                                                            CASE
                                                                WHEN bd.billType = 'Pick Up' THEN CONCAT('P',btd.tokenNo)
                                                                WHEN bd.billType = 'Delivery' THEN CONCAT('D',btd.tokenNo)
                                                                WHEN bd.billType = 'Dine In' THEN CONCAT('R',btd.tokenNo)
                                                                ELSE NULL
                                                            END AS tokenNo,
                                                            bwu.onlineId AS onlineId,
                                                            boud.holderName AS holderName,
                                                            boud.upiId AS upiId,
                                                            bd.firmId AS firmId, 
                                                            bd.cashier AS cashier, 
                                                            bd.menuStatus AS menuStatus, 
                                                            bd.billType AS billType, 
                                                            bd.billPayType AS billPayType, 
                                                            bd.discountType AS discountType, 
                                                            bd.discountValue AS discountValue, 
                                                            bd.totalDiscount AS totalDiscount, 
                                                            bd.totalAmount AS subTotal, 
                                                            bd.settledAmount AS settledAmount, 
                                                            bd.billComment AS billComment, 
                                                            DATE_FORMAT(bd.billDate,'%d/%m/%Y') AS billDate,
                                                            bd.billStatus AS billStatus,
                                                            DATE_FORMAT(bd.billCreationDate,'%h:%i %p') AS billTime,
                                                            bwcd.billFooterNote AS footerBill,
                                                            bwcd.appriciateLine AS appriciateLine
                                                        FROM 
                                                            billing_data AS bd
                                                        LEFT JOIN billing_Official_data AS bod ON bod.billId = bd.billId
                                                        LEFT JOIN billing_Complimentary_data AS bcd ON bcd.billId = bd.billId
                                                        LEFT JOIN billing_token_data AS btd ON btd.billId = bd.billId
                                                        LEFT JOIN billing_firm_data AS bfd ON bfd.firmId = bd.firmId
                                                        LEFT JOIN billing_billWiseUpi_data AS bwu ON bwu.billId = bd.billId
                                                        LEFT JOIN billing_onlineUPI_data AS boud ON boud.onlineId = bwu.onlineId
                                                        LEFT JOIN billing_category_data AS bcgd ON bcgd.categoryName = bd.billType
                                                        LEFT JOIN billing_branchWiseCategory_data AS bwcd ON bwcd.categoryId = bcgd.categoryId
                                                        WHERE bd.billId = '${billId}'`;
                        let sql_query_getBillwiseItem = `SELECT
                                             bwid.iwbId AS iwbId,
                                             bwid.itemId AS itemId,
                                             imd.itemName AS itemName,
                                             imd.itemCode AS inputCode,
                                             SUM(bwid.qty) AS qty,
                                             bwid.unit AS unit,
                                             bwid.itemPrice AS itemPrice,
                                             SUM(bwid.price) AS price,
                                             bwid.comment AS comment
                                         FROM
                                             billing_billWiseItem_data AS bwid
                                         INNER JOIN item_menuList_data AS imd ON imd.itemId = bwid.itemId
                                         WHERE bwid.billId = '${billId}'
                                         GROUP BY bwid.itemId, bwid.unit`;
                        let sql_query_getItemWiseAddons = `SELECT
                                               iwad.iwaId AS iwaId,
                                               iwad.iwbId AS iwbId,
                                               iwad.addOnsId AS addOnsId,
                                               iad.addonsName AS addonsName,
                                               iad.addonsGujaratiName AS addonsGujaratiName,
                                               iad.price AS addonPrice
                                           FROM
                                               billing_itemWiseAddon_data AS iwad
                                           LEFT JOIN item_addons_data AS iad ON iad.addonsId = iwad.addOnsId
                                           WHERE iwad.iwbId IN(SELECT COALESCE(bwid.iwbId, NULL) FROM billing_billWiseItem_data AS bwid WHERE bwid.billId = '${billId}')`;
                        let sql_query_getCustomerInfo = `SELECT
                                             bwcd.bwcId AS bwcId,
                                             bwcd.customerId AS customerId,
                                             bwcd.mobileNo AS mobileNo,
                                             bwcd.addressId AS addressId,
                                             bwcd.address AS address,
                                             bwcd.locality AS locality,
                                             bwcd.customerName AS customerName
                                         FROM
                                             billing_billWiseCustomer_data AS bwcd
                                         WHERE bwcd.billId = '${billId}'`;
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
                                        firmId = (SELECT firmId FROM billing_data WHERE billId = '${billId}')`;
                        let sql_query_getTableData = `SELECT
                                                            bwtn.tableNo AS tableNo,
                                                            bwtn.areaId AS areaId,
                                                            bwtn.assignCaptain AS assignCaptain,
                                                            dia.areaName AS areaName,
                                                            IFNULL(CONCAT(dia.prefix, ' ', bwtn.tableNo), bwtn.tableNo) AS displayTableNo
                                                          FROM
                                                            billing_billWiseTableNo_data AS bwtn
                                                          LEFT JOIN billing_dineInArea_data dia ON dia.areaId = bwtn.areaId
                                                          WHERE billId = '${billId}'`;
                        let sql_query_getSubTokens = `SELECT subTokenNumber FROM billing_subToken_data WHERE billId = '${billId}'`;

                        const sql_query_getBillData = `${sql_query_getBillingData};
                                       ${sql_query_getBillwiseItem};
                                       ${sql_query_getFirmData};
                                       ${sql_query_getItemWiseAddons};
                                       ${sql_query_getCustomerInfo};
                                       ${sql_query_getTableData};
                                       ${sql_query_getSubTokens}`;

                        let sql_query_chkBillExist = `SELECT billId, billType, billPayType FROM billing_data WHERE billId = '${billId}';
                                                       SELECT adminMacAddress FROM billing_admin_data`;
                        connection.query(sql_query_chkBillExist, (err, bill) => {
                            if (err) {
                                console.error("An error occurred in SQL Query", err);
                                connection.rollback(() => {
                                    connection.release();
                                    return res.status(500).send('Database Error');
                                });
                            } else {
                                if (!bill || !bill[0] || bill[0].length === 0) {
                                    connection.rollback(() => {
                                        connection.release();
                                        return res.status(404).send('Bill Id Not Found');
                                    });
                                } else {
                                    const billType = bill[0][0].billType;
                                    const billPayType = bill[0][0].billPayType;
                                    const adminMacAddress = bill[1] && bill[1][0] ? bill[1][0].adminMacAddress : null;

                                    connection.query(sql_query_getBillData, (err, billData) => {
                                        if (err) {
                                            console.error("An error occurred in SQL Query", err);
                                            connection.rollback(() => {
                                                connection.release();
                                                return res.status(500).send('Database Error');
                                            });
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
                                                itemsData: newItemJson,
                                                firmData: billData && billData[2] ? billData[2][0] : [],
                                                ...(['Pick Up', 'Delivery', 'Dine In'].includes(billType) ? { customerDetails: billData && billData[4][0] ? billData[4][0] : '' } : ''),
                                                ...(billType === 'Dine In' ? { tableInfo: billData[5][0] } : ''),
                                                subTokens: billData && billData[5] && billData[6].length ? billData[6].map(item => item.subTokenNumber).sort((a, b) => a - b).join(", ") : null,
                                                ...(['online'].includes(billPayType) ? {
                                                    "upiJson": {
                                                        "onlineId": billData[0][0].onlineId,
                                                        "holderName": billData[0][0].holderName,
                                                        "upiId": billData[0][0].upiId
                                                    }
                                                } : '')
                                            }
                                            connection.commit((err) => {
                                                if (err) {
                                                    console.error("Error committing transaction:", err);
                                                    connection.rollback(() => {
                                                        connection.release();
                                                        return res.status(500).send('Database Error');
                                                    });
                                                } else {
                                                    connection.release();
                                                    if (adminMacAddress) {
                                                        req?.io?.emit(`print_Bill_${adminMacAddress}`, json);
                                                    }
                                                    return res.status(200).send(json);
                                                }
                                            });
                                        }
                                    });
                                }
                            }
                        })
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
            });
        }
    });
}

// Make me as a Admin

const makeMeAdmin = (req, res) => {
    try {
        let token;
        token = req.headers ? req.headers.authorization.split(" ")[1] : null;
        if (token) {
            const decoded = jwt.verify(token, process.env.JWT_SECRET);
            const userId = decoded.id.id;
            const cashier = decoded.id.firstName;
            const userRights = decoded.id.rights;
            const macAddress = req.query.macAddress;
            const adminPassword = req.query.adminPassword;
            const uid1 = new Date();
            const adminId = String("admin_" + uid1.getTime());
            if (userRights == 1) {
                if (!macAddress || !adminPassword) {
                    return res.status(404).send('Please Fill All The Fields....!')
                } else {
                    const sql_querry_authuser = `SELECT * FROM user_details WHERE userId = '${userId}'`;
                    pool.query(sql_querry_authuser, (err, data) => {
                        console.log(data)
                        if (err) {
                            console.error("An error occurred in SQL Queery", err);
                            return res.status(500).send('Database Error');
                        } else if (data[0] && data[0].password == adminPassword) {
                            console.log(data)
                            const sql_query_removeOldAdmin = `TRUNCATE TABLE billing_admin_data`;
                            pool.query(sql_query_removeOldAdmin, (err, data) => {
                                if (err) {
                                    console.error("An error occurred in SQL Queery", err);
                                    return res.status(500).send('Database Error');
                                } else {
                                    const sql_query_makeAdmin = `INSERT INTO billing_admin_data(adminId, adminMacAddress, adminBy)
                                                                 VALUES('${adminId}', '${macAddress}', '${cashier}')`;
                                    pool.query(sql_query_makeAdmin, (err, data) => {
                                        if (err) {
                                            console.error("An error occurred in SQL Queery", err);
                                            return res.status(500).send('Database Error');
                                        } else {
                                            return res.status(200).send("Set Admin Succeess");
                                        }
                                    })
                                }
                            })
                        } else {
                            return res.status(400).send("Invalid Password");
                        }
                    })
                }
            } else {
                return res.status(400).send('Only Owner Can Make Admin');
            }
        } else {
            return res.status(404).send('Please Login First....!');
        }
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).json('Internal Server Error');
    }
}

// Get Admin Server ID

const getAdminServerId = (req, res) => {
    try {
        let sql_query_getAdminId = `SELECT adminId, adminMacAddress, adminBy FROM billing_admin_data`;
        pool.query(sql_query_getAdminId, (err, data) => {
            if (err) {
                console.error("An error occurred in SQL Queery", err);
                return res.status(500).send('Database Error');
            } else {
                return res.status(200).send(data[0]);
            }
        })
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).json('Internal Server Error');
    }
}


module.exports = {
    // Get Bill Data
    getBillingStaticsData,
    getBillDataById,
    getRecentBillData,
    getBillDataByToken,
    getLiveViewByCategoryId,

    // Add Bill Data
    addPickUpBillData,
    addDeliveryBillData,

    // Update Bill Data
    updatePickUpBillData,
    updateDeliveryBillData,
    updateBillStatusById,

    // Print Bill Data
    printBillInAdminSystem,
    makeMeAdmin,
    getAdminServerId
}