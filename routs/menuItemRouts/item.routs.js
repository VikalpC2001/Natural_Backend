const express = require('express');
const router = express.Router();
const { protect } = require("../../middlewares/authMiddlewares.js");

// Sub-Category Routs

const subCategoryController = require("../../controller/menuItemController/subCategory.controller.js");

router.get('/getSubCategoryList', protect, subCategoryController.getSubCategoryList);
router.get('/ddlSubCategory', subCategoryController.ddlSubCategory);
router.post('/addSubCategoryData', protect, subCategoryController.addSubCategoryData);
router.delete('/removeSubCategoryData', protect, subCategoryController.removeSubCategoryData);
router.post('/updateSubCategoryData', protect, subCategoryController.updateSubCategoryData);
router.post('/addSubCategoryPeriod', protect, subCategoryController.addSubCategoryPeriod);
router.post('/updateSubCategoryPeriod', protect, subCategoryController.updateSubCategoryPeriod);
router.get('/getSubCategoryListForMobile', subCategoryController.getSubCategoryListForMobile);


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
router.post('/addItemData', itemController.addItemData);
router.delete('/removeItemData', protect, itemController.removeItemData);
router.post('/updateItemData', protect, itemController.updateItemData);
router.post('/updateMultipleItemPrice', protect, itemController.updateMultipleItemPrice);
router.get('/updateItemStatus', protect, itemController.updateItemStatus);
router.get('/getItemSalesReport', itemController.getItemSalesReport);
router.post('/updateItemPriceByMenuId', itemController.updateItemPriceByMenuId);
router.get('/exportPdfForItemSalesReport', itemController.exportPdfForItemSalesReport);
router.get('/getItmeDataForTouchView', itemController.getItmeDataForTouchView);
router.get('/getItemDataByCode', itemController.getItemDataByCode);

// Addon Group Routs

const addonGroupController = require("../../controller/menuItemController/addonGroup.controller.js");

router.get('/getAddOnsGroupList', protect, addonGroupController.getAddOnsGroupList);
router.post('/addAddOnsGroupData', protect, addonGroupController.addAddOnsGroupData);
router.delete('/removeAddOnsGgroupData', protect, addonGroupController.removeAddOnsGgroupData);
router.post('/updateAddOnsGroupData', protect, addonGroupController.updateAddOnsGroupData);
router.get('/getItemListByAddon', protect, addonGroupController.getItemListByAddon);
router.post('/assignAddonGroup', protect, addonGroupController.assignAddonGroup);

// Addon Routs

const addonController = require("../../controller/menuItemController/addon.controller.js");

router.get('/getAddOnsList', protect, addonController.getAddOnsList);
router.post('/addAddOnsData', protect, addonController.addAddOnsData);
router.delete('/removeAddOnsData', protect, addonController.removeAddOnsData);
router.post('/updateAddOnsData', protect, addonController.updateAddOnsData);

module.exports = router;