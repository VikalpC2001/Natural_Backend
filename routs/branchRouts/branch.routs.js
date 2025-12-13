const express = require('express');
const router = express.Router();
const { protect } = require("../../middlewares/authMiddlewares.js");

// Branch Routs

const branchController = require("../../controller/branchController/branch.controller.js");

router.get('/getBranchList', protect, branchController.getBranchList);
router.post('/addBranch', protect, branchController.addBranch);
router.delete('/removeBranch', protect, branchController.removeBranch);
router.post('/updateBranch', protect, branchController.updateBranch);

module.exports = router;