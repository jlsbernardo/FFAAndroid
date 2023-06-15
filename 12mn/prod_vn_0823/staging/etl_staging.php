<?php
ini_set('memory_limit', '-1');
ini_set('sqlsrv.ClientBufferMaxKBSize','512000'); // Setting to 512M
ini_set('pdo_sqlsrv.client_buffer_max_kb_size','512000'); // Setting to 512M - for pdo_sqlsrv

require_once dirname(__DIR__).'/staging/src/init_loader.php';
require_once dirname(__DIR__).'/staging/etl/demo/Demo.php';
require_once dirname(__DIR__).'/staging/etl/farmer_meeting/Meeting.php';
require_once dirname(__DIR__).'/staging/etl/retailer_visit/Retailer.php';
require_once dirname(__DIR__).'/staging/etl/users/User.php';
require_once dirname(__DIR__).'/staging/etl/geo_hierarchy/Region.php';
require_once dirname(__DIR__).'/staging/etl/geo_hierarchy/Zone.php';
require_once dirname(__DIR__).'/staging/etl/geo_hierarchy/Territory.php';
require_once dirname(__DIR__).'/staging/etl/portal_settings/PortalSetting.php';
require_once dirname(__DIR__).'/staging/etl/teams/Teams.php';
require_once dirname(__DIR__).'/staging/etl/staging/Staging.php';
require_once dirname(__DIR__).'/staging/etl/complaints/Complaint.php';
require_once dirname(__DIR__).'/staging/etl/crops/Crop.php';
require_once dirname(__DIR__).'/staging/etl/products/Product.php';
require_once dirname(__DIR__).'/staging/etl/target_activities/TargetActivity.php';

/**
 * This script is for generating .sql and .txt file for staging (for all countries)
 * That will be imported to Analytical Database using any database tools ie. MySQL Workbench
 * 
 * @return \Logs
 */
function run()
{
    include dirname(__DIR__).'/staging/config/countries.php';
    
    echo Logs::success("ETL Staging Process Starts: " . date('Y-m-d H:i:s') . "\n");

    $txt = "";

    foreach ($countries as $country) {

        //staging
        $staging = new Staging($country, 'analytical');
        $stagingResults = $staging->deleteStaging();

        //demo
        echo Logs::success("ETL Demo Process Start: " . date('Y-m-d H:i:s') . "\n");
        $demo = new Demo($country);
        $demoResults = $demo->getDataFromFFA();
        
        $demoQueryResults =  $demoResults['data'];

        if (is_array($demoQueryResults)) {
            $demoAnalytical = new Demo($country, 'analytical');

            $insertDemo = $demoAnalytical->insertIntoStaging($demoQueryResults, $demoResults['last_inserted']);
            $demoCounts = $insertDemo['count'];
        } else {
            $demoCounts = $demoQueryResults;
        }
        echo Logs::success("ETL Demo Process End: " . date('Y-m-d H:i:s') . "\n");
        
        //meeting
        echo Logs::success("ETL Meeting Proces Start: " . date('Y-m-d H:i:s') . "\n");
        $meeting = new Meeting($country);
        $meetingResults = $meeting->getDataFromFFA();
        $meetingQueryResults = $meetingResults['data'];
        
        if (is_array($meetingQueryResults)) {

            $meetingAnalytical = new Meeting($country, 'analytical');
            $insertMeeting = $meetingAnalytical->insertIntoStaging($meetingQueryResults, $meetingResults['last_inserted']);
            $meetingCounts = $insertMeeting['count'];
        } else {
            $meetingCounts = $meetingQueryResults;
        }

        echo Logs::success("ETL Meeting Process End: " . date('Y-m-d H:i:s') . "\n");

        //retailer
        echo Logs::success("ETL Retailer Proces Start: " . date('Y-m-d H:i:s') . "\n");
        $retailer = new Retailer($country);
        $retailerResults = $retailer->getDataFromFFA();
        $retailerQueryResults =  $retailerResults['data'];

        if (is_array($retailerQueryResults)) {
            $retailerAnalytical = new Retailer($country, 'analytical');
            $insertRetailer = $retailerAnalytical->insertIntoStaging($retailerQueryResults, $retailerResults['last_inserted']);
            $retailerCounts = $insertRetailer['count'];
        } else {
            $retailerCounts = $retailerQueryResults;
        }
        echo Logs::success("ETL Meeting Process End: " . date('Y-m-d H:i:s') . "\n");

        //users
        echo Logs::success("ETL User Process Start: " . date('Y-m-d H:i:s') . "\n");
        $users = new User($country);
        $usersResults = $users->getDataFromFFA();
        $userQueryResults = $usersResults['data'];

        if (is_array($userQueryResults)) {
            $usersAnalytical = new User($country, 'analytical');
            $insertUsers = $usersAnalytical->insertIntoStaging($userQueryResults, $usersResults['last_inserted']);
            $usersCounts = $insertUsers['count'];
        } else {
            $usersCounts = $userQueryResults;
        }
        echo Logs::success("ETL User Process end: " . date('Y-m-d H:i:s') . "\n");

        //geo_hierarchy_region
        echo Logs::success("ETL Region Process Start: " . date('Y-m-d H:i:s') . "\n");
        $region = new Region($country);
        $regionResults = $region->getDataFromFFA();

        $regionQueryResults = $regionResults['data'];

        if (is_array($regionQueryResults)) {
            $regionAnalytical = new Region($country, 'analytical');
            $insertRegion = $regionAnalytical->insertIntoStaging($regionQueryResults);
            $regionCounts = $insertRegion['count'];
        } else {
            $regionCounts = $regionQueryResults;
        }
        echo Logs::success("ETL Region Process end: " . date('Y-m-d H:i:s') . "\n");

        //geo_hierarchy_zone
        echo Logs::success("ETL Zone Process Start: " . date('Y-m-d H:i:s') . "\n");
        $zone = new Zone($country);
        $zoneResults = $zone->getDataFromFFA();

        $zoneQueryResults = $zoneResults['data'];

        if (is_array($zoneQueryResults)) {
            $zoneAnalytical = new Zone($country, 'analytical');
            $insertZone = $zoneAnalytical->insertIntoStaging($zoneQueryResults);
            $zoneCounts = $insertZone['count'];
        } else {
            $zoneCounts = $zoneQueryResults;
        }
        echo Logs::success("ETL Zone Process end: " . date('Y-m-d H:i:s') . "\n");

        //geo_hierarchy_territory
        echo Logs::success("ETL Territory Process Start: " . date('Y-m-d H:i:s') . "\n");
        $territory = new Territory($country);
        $territoryResults = $territory->getDataFromFFA();

        $territoryQueryResults = $territoryResults['data'];

        if (is_array($territoryQueryResults)) {
            $territoryAnalytical = new Territory($country, 'analytical');
            $insertZone = $territoryAnalytical->insertIntoStaging($territoryQueryResults);
            $territoryCounts = $insertZone['count'];
        } else {
            $territoryCounts = $territoryQueryResults;
        }
        echo Logs::success("ETL Territory Process End: " . date('Y-m-d H:i:s') . "\n");

        //portal settings
        echo Logs::success("ETL PortalSetting Process Start: " . date('Y-m-d H:i:s') . "\n");
        $portalSettings = new PortalSetting($country);
        $portalSettingsResults = $portalSettings->getDataFromFFA();

        $portalSettingsQueryResults = $portalSettingsResults['data'];

        if (is_array($portalSettingsQueryResults)) {
            $portalSettingsAnalytical = new PortalSetting($country, 'analytical');
            $insertZone = $portalSettingsAnalytical->insertIntoStaging($portalSettingsQueryResults);
            $portalSettingsCounts = $insertZone['count'];
        } else {
            $portalSettingsCounts = $portalSettingsQueryResults;
        }
        echo Logs::success("ETL PortalSetting Process End: " . date('Y-m-d H:i:s') . "\n");

        //teams
        echo Logs::success("ETL Teams Process Start: " . date('Y-m-d H:i:s') . "\n");
        $teams = new Teams($country);
        $teamsResults = $teams->getDataFromFFA();

        $teamsQueryResults = $teamsResults['data'];

        if (is_array($teamsQueryResults)) {
            $teamsAnalytical = new Teams($country, 'analytical');
            $insertZone = $teamsAnalytical->insertIntoStaging($teamsQueryResults);
            $teamsCounts = $insertZone['count'];
        } else {
            $teamsCounts = $teamsQueryResults;
        }
        echo Logs::success("ETL Teams Process End: " . date('Y-m-d H:i:s') . "\n");

        //complaints
        echo Logs::success("ETL Complaint Process Start: " . date('Y-m-d H:i:s') . "\n");
        $complaints = new Complaint($country);
        $complaintsResults = $complaints->getDataFromFFA();

        $complaintsQueryResults = $complaintsResults['data'];

        if (is_array($complaintsQueryResults)) {
            $complaintsAnalytical = new Complaint($country, 'analytical');
            $insertZone = $complaintsAnalytical->insertIntoStaging($complaintsQueryResults);
            $complaintsCounts = $insertZone['count'];
        } else {
            $complaintsCounts = $complaintsQueryResults;
        }
        echo Logs::success("ETL Teams Process End: " . date('Y-m-d H:i:s') . "\n");

        //crops
        echo Logs::success("ETL Crop Process Start: " . date('Y-m-d H:i:s') . "\n");
        $crops = new Crop($country);
        $cropsResults = $crops->getDataFromFFA();

        $cropsQueryResults = $cropsResults['data'];

        if (is_array($cropsQueryResults)) {
            $cropsAnalytical = new Crop($country, 'analytical');
            $insertZone = $cropsAnalytical->insertIntoStaging($cropsQueryResults);
            $cropsCounts = $insertZone['count'];
        } else {
            $cropsCounts = $cropsQueryResults;
        }
        echo Logs::success("ETL Crop Process End: " . date('Y-m-d H:i:s') . "\n");

        //product
        echo Logs::success("ETL Product Process Start: " . date('Y-m-d H:i:s') . "\n");
        $products = new Product($country);
        $productsResults = $products->getDataFromFFA();

        $productsQueryResults = $productsResults['data'];

        if (is_array($productsQueryResults)) {
            $productsAnalytical = new Product($country, 'analytical');
            $insertZone = $productsAnalytical->insertIntoStaging($productsQueryResults);
            $productsCounts = $insertZone['count'];
        } else {
            $productsCounts = $productsQueryResults;
        }
        echo Logs::success("ETL Product Process End: " . date('Y-m-d H:i:s') . "\n");     

        //target activities
        echo Logs::success("ETL TargetActivity Process Start: " . date('Y-m-d H:i:s') . "\n");
        $targetActivity = new TargetActivity($country);
        $targetActivityResults = $targetActivity->getDataFromFFA();

        $targetActivityQueryResults = isset($targetActivityResults['data']) ? $targetActivityResults['data']: 0;

        if (is_array($targetActivityQueryResults)) {
            $targetActivityAnalytical = new TargetActivity($country, 'analytical');
            $insertZone = $targetActivityAnalytical->insertIntoStaging($targetActivityQueryResults);
            $targetActivityCounts = $insertZone['count'];
        } else {
            $targetActivityCounts = $targetActivityQueryResults;
        }
        echo Logs::success("ETL TargetActivity Process End: " . date('Y-m-d H:i:s') . "\n");

        $countryName = $country['country_name'];

        $txt =
        "Countries: $countryName\n" .
        "Demo Counts: $demoCounts\n".
        "Meeting Counts: $meetingCounts\n".
        "Retailer Counts: $retailerCounts\n".
        "User Counts: $usersCounts\n".
        "Region Counts: $regionCounts\n".
        "Zone Counts: $zoneCounts\n".
        "Territory Counts: $territoryCounts\n".
        "Portal Settings Counts: $portalSettingsCounts\n".
        "Teams Counts: $teamsCounts\n".
        "Complaints Counts: $complaintsCounts\n".
        "Crops Counts: $cropsCounts\n".
        "Products Counts: $productsCounts\n\n".
        "Target Activity Counts: $targetActivityCounts\n\n"
        ;

        echo $txt;
    }

    echo Logs::success("ETL Staging Process Completed: " . date('Y-m-d H:i:s') . "\n");
}

run();