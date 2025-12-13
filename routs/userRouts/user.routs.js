const express = require('express')
const router = express.Router();
const { protect } = require("../../middlewares/authMiddlewares.js");

// User Routs

const userController = require("../../controller/userController/user.controller.js");

router.post('/authUser', userController.authUser);
router.post('/getNewTokenByBranchId', userController.getNewTokenByBranchId);
router.post('/getNewTokenByMfProductCategoryId', userController.getNewTokenByMfProductCategoryId);
router.post('/addUserDetailsByBranchOwner', userController.addUserDetailsByBranchOwner);
router.post('/addUserDetailsByOwner', userController.addUserDetailsByOwner);
router.get('/getUserDetails', protect, userController.getUserDetails);
router.get('/ddlRights', protect, userController.ddlRights);
router.delete('/removeUser', protect, userController.removeUserDetails);
router.post('/updateUserDetailsByOwner', protect, userController.updateUserDetailsByOwner);
router.post('/addUserDetailsByBranchOwner', protect, userController.addUserDetailsByBranchOwner);
router.post('/updateUserDetailsByBranchOwner', protect, userController.updateUserDetailsBranchOwner);
router.get('/fillUserDetails', protect, userController.fillUserDetails);
router.post('/chkPassword', protect, userController.chkPassword);

// Unit Routs

const unitController = require("../../controller/userController/unit.controller.js");

router.get('/getUnit', protect, unitController.getUnit);
router.post('/addUnit', protect, unitController.addUnit);
router.post('/updateUnit', protect, unitController.updateUnit);

module.exports = router;