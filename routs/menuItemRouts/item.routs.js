const express = require('express');
const router = express.Router();
const { protect } = require("../../middlewares/authMiddlewares.js");

// Sub-Category Routs

const subCategoryController = require("../../controller/menuItemController/subCategory.controller.js");

router.get('/getSubCategoryList', protect, subCategoryController.getSubCategoryList);
router.get('/ddlSubCategory', protect, subCategoryController.ddlSubCategory);
router.post('/addSubCategoryData', protect, subCategoryController.addSubCategoryData);
router.delete('/removeSubCategoryData', protect, subCategoryController.removeSubCategoryData);
router.post('/updateSubCategoryData', protect, subCategoryController.updateSubCategoryData);
router.post('/addSubCategoryPeriod', protect, subCategoryController.addSubCategoryPeriod);
router.post('/updateSubCategoryPeriod', protect, subCategoryController.updateSubCategoryPeriod);
router.get('/getSubCategoryListForMobile', subCategoryController.getSubCategoryListForMobile);
router.post('/updateDisplayRankForSubCategory', protect, subCategoryController.updateDisplayRankForSubCategory);


// Menu Category Routs

const menuCategoryController = require("../../controller/menuItemController/menuCategory.controller.js");

router.get('/getMenuCategory', protect, menuCategoryController.getMenuCategory);
router.post('/addMenuCategory', protect, menuCategoryController.addMenuCategory);
router.delete('/removeMenuCategory', protect, menuCategoryController.removeMenuCategory);
router.post('/updateMenuCategory', protect, menuCategoryController.updateMenuCategory);
router.get('/copyPriceAndStatusByMenuId', protect, menuCategoryController.copyPriceAndStatusByMenuId);

// Item Routs

const itemController = require("../../controller/menuItemController/item.controller.js");

router.get('/getItemData', protect, itemController.getItemData);
router.post('/addItemData', protect, itemController.addItemData);
router.delete('/removeItemData', protect, itemController.removeItemData);
router.post('/updateItemData', protect, itemController.updateItemData);
router.post('/updateMultipleItemPrice', protect, itemController.updateMultipleItemPrice);
router.get('/updateItemStatus', protect, itemController.updateItemStatus);
router.get('/getItemSalesReport', protect, itemController.getItemSalesReport);
router.post('/updateItemPriceByMenuId', protect, itemController.updateItemPriceByMenuId);
router.get('/exportPdfForItemSalesReport', protect, itemController.exportPdfForItemSalesReport);
router.get('/getItmeDataForTouchView', protect, itemController.getItmeDataForTouchView);
router.get('/getItemDataByCode', protect, itemController.getItemDataByCode);
router.post('/addFavouritemByBranch', protect, itemController.addFavouritemByBranch);

// Addon Group Routs

const addonController = require("../../controller/menuItemController/addon.controller.js");

router.get('/getAddOnsGroupList', protect, addonController.getAddOnsGroupList);
router.post('/addAddonGroupData', protect, addonController.addAddonGroupData);
router.delete('/removeAddonGroupData', protect, addonController.removeAddonGroupData);
router.post('/updateAddonGroupData', protect, addonController.updateAddonGroupData);
router.get('/getItemListByAddon', protect, addonController.getItemListByAddon);
router.post('/assignAddonGroup', protect, addonController.assignAddonGroup);

module.exports = router;