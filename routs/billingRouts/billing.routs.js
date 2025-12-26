const express = require('express');
const router = express.Router();
const { protect } = require("../../middlewares/authMiddlewares.js");

// Billing Category Routs

const categoryController = require("../../controller/billingController/billCategory.controller.js");

router.get('/getBillCategory', protect, categoryController.getBillCategory);
router.post('/updateBillCategoryData', protect, categoryController.updateBillCategoryData);
router.get('/ddlBillCategory', protect, categoryController.ddlBillCategory);

// Comment Routs

const commentController = require("../../controller/billingController/comment.controller.js");

router.get('/getComment', protect, commentController.getComment);
router.get('/getCommentData', protect, commentController.getCommentData);
router.post('/addComment', protect, commentController.addComment);
router.delete('/removeComment', protect, commentController.removeComment);
router.post('/updateComment', protect, commentController.updateComment);

// Customer Routs

const customerController = require("../../controller/billingController/customer.controller.js");

router.get('/searchCustomerData', protect, customerController.searchCustomerData);
router.get('/getCustomerList', protect, customerController.getCustomerList);
router.get('/getCustomerDetailsById', protect, customerController.getCustomerDetailsById);
router.post('/addMultipleCustomerData', customerController.addMultipleCustomerData);
router.post('/addCustomerData', customerController.addCustomerData);
router.delete('/removeCustomeData', customerController.removeCustomeData);
router.post('/updateCustomerData', customerController.updateCustomerData);
router.get('/getStaticsByCustomer', customerController.getStaticsByCustomer);
router.get('/getBillDataBycustomerId', customerController.getBillDataBycustomerId);

// Firm Routs

const firmController = require("../../controller/billingController/firm.controller.js");

router.get('/getFirmData', protect, firmController.getFirmData);
router.get('/getFirmDataById', protect, firmController.getFirmDataById);
router.post('/addFirmData', protect, firmController.addFirmData);
router.delete('/removeFirmData', protect, firmController.removeFirmData);
router.post('/updateFirmData', protect, firmController.updateFirmData);
router.get('/ddlFirmData', protect, firmController.ddlFirmData);
router.get('/getTaxReportByFirmId', protect, firmController.getTaxReportByFirmId);
router.get('/getBillDataByFirmId', protect, firmController.getBillDataByFirmId);
router.get('/getCancelBillDataByFirmId', protect, firmController.getCancelBillDataByFirmId);
router.get('/getComplimentaryBillDataByFirmId', protect, firmController.getComplimentaryBillDataByFirmId);
router.get('/getMonthWiseBillDataByFirmId', protect, firmController.getMonthWiseBillDataByFirmId);
router.get('/getStaticsDataByFirmId', protect, firmController.getStaticsDataByFirmId);

// Billing Routs

//dine in routes
const dineInController = require("../../controller/billingController/dineIn.controller.js");

router.get('/getSubTokensByBillId', protect, dineInController.getSubTokensByBillId);
router.get('/getAllTableView', protect, dineInController.getAllTableView);
router.post('/addDineInOrder', protect, dineInController.addDineInOrder);
router.delete('/removeSubTokenDataById', protect, dineInController.removeSubTokenDataById);
router.post('/updateSubTokenDataById', protect, dineInController.updateSubTokenDataById);
router.get('/manageTableByArea', protect, dineInController.manageTableByArea);
router.get('/printTableBill', protect, dineInController.printTableBill);
router.post('/updateDineInBillData', protect, dineInController.updateDineInBillData);
router.post('/sattledBillDataByID', protect, dineInController.sattledBillDataByID);
router.post('/cancelBillDataByID', protect, dineInController.cancelBillDataByID);
router.get('/moveTable', protect, dineInController.moveTable);
router.get('/isTableEmpty', protect, dineInController.isTableEmpty);
router.get('/sattledCancelTokenTable', protect, dineInController.sattledCancelTokenTable);

const billingController = require("../../controller/billingController/billing.controller.js");

//Get Billing Data
router.get('/getBillingStaticsData', protect, billingController.getBillingStaticsData);
router.get('/getBillDataById', protect, billingController.getBillDataById);
router.get('/getRecentBillData', protect, billingController.getRecentBillData);
router.get('/getBillDataByToken', protect, billingController.getBillDataByToken);
router.get('/getLiveViewByCategoryId', protect, billingController.getLiveViewByCategoryId);

// Add Billing Data
router.post('/addPickUpBillData', protect, billingController.addPickUpBillData);
router.post('/addDeliveryBillData', protect, billingController.addDeliveryBillData);

// Update Billing Data
router.post('/updatePickUpBillData', protect, billingController.updatePickUpBillData);
router.post('/updateDeliveryBillData', protect, billingController.updateDeliveryBillData);
router.get('/updateBillStatusById', protect, billingController.updateBillStatusById);

// Print Bill Data
router.get('/printBillInAdminSystem', protect, billingController.printBillInAdminSystem);

// Hold Billing Routs

const holdController = require("../../controller/billingController/hold.controller.js");

router.get('/getHoldCount', protect, holdController.getHoldCount);
router.get('/getHoldBillData', protect, holdController.getHoldBillData);
router.get('/getHoldBillDataById', protect, holdController.getHoldBillDataById);
router.post('/addPickUpHoldBillData', protect, holdController.addPickUpHoldBillData);
router.post('/addDeliveryHoldBillData', protect, holdController.addDeliveryHoldBillData);
router.delete('/discardHoldData', protect, holdController.discardHoldData);

// Printer Routs

const printerController = require("../../controller/billingController/printer.controller.js");

router.get('/getPrinterList', protect, printerController.getPrinterList);
router.post('/updatePrinterData', protect, printerController.updatePrinterData);

// UPI Routs

const upiConntroller = require("../../controller/billingController/upi.controller.js");

router.get('/getUPIList', protect, upiConntroller.getUPIList);
router.post('/addUPI', protect, upiConntroller.addUPI);
router.delete('/removeUPI', protect, upiConntroller.removeUPI);
router.post('/updateUPI', protect, upiConntroller.updateUPI);
router.get('/ddlUPI', protect, upiConntroller.ddlUPI);
router.get('/setDefaultUPI', protect, upiConntroller.setDefaultUPI);
router.get('/getUPITransactionById', protect, upiConntroller.getUPITransactionById);
router.get('/getUPIStaticsById', protect, upiConntroller.getUPIStaticsById);

module.exports = router;