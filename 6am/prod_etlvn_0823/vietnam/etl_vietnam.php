<?php
ini_set('memory_limit', '-1');
ini_set('sqlsrv.ClientBufferMaxKBSize','512000'); // Setting to 512M
ini_set('pdo_sqlsrv.client_buffer_max_kb_size','512000'); // Setting to 512M - for pdo_sqlsrv

require_once dirname(__DIR__) . '/vietnam/src/init_loader.php';
require_once dirname(__DIR__) . '/vietnam/etl/demo/Demo.php';
require_once dirname(__DIR__) . '/vietnam/etl/farmer_meeting/Meeting.php';
require_once dirname(__DIR__) . '/vietnam/etl/retailer_visit/Retailer.php';
require_once dirname(__DIR__) . '/vietnam/etl/geo_hierarchy/Region.php';
require_once dirname(__DIR__) . '/vietnam/etl/geo_hierarchy/Territory.php';
require_once dirname(__DIR__) . '/vietnam/etl/geo_hierarchy/Zone.php';
require_once dirname(__DIR__) . '/vietnam/etl/teams/Teams.php';
require_once dirname(__DIR__) . '/vietnam/etl/users/User.php';
require_once dirname(__DIR__) . '/vietnam/etl/portal_settings/PortalSetting.php';
require_once dirname(__DIR__) . '/vietnam/etl/complaints/Complaint.php';
require_once dirname(__DIR__) . '/vietnam/etl/crops/Crop.php';
require_once dirname(__DIR__) . '/vietnam/etl/products/Product.php';
require_once dirname(__DIR__) . '/vietnam/etl/target_activities/TargetActivity.php';

/**
 * This script is for boilerplate of reports scripts per country (ie. demo_reports_ph.php, demo_reports_vn.php etc.)
 * 
 * @return \Logs
 */
function run()
{
    include dirname(__DIR__) . '/vietnam/config/countries.php';

    echo Logs::success("ETL Vietnam Process Starts: " . date('Y-m-d H:i:s') . "\n");

    $queryResults = "";
    $txt = "";

    /** Demo **/
    $demo = new Demo($countries[0], 'analytical');
    $demoResults = $demo->getStaging();

    if (is_array($demoResults)) {
        $demoCounts =  $demoResults['num_rows'];
        $demoUpdateFFA = new Demo($countries[0], 'ffa');
        $demoUpdateFFAResults = $demoUpdateFFA->updateRNAEtlSync($demoResults['last_insert_id'], $demoResults['num_rows']);

    } else {
        $demoCounts = $demoResults;
    }
    /** End of Demo **/

    /** Farmer Meeting **/
    $meeting = new Meeting($countries[0], 'analytical');
    $meetingResults = $meeting->getStaging();
    
    if (is_array($meetingResults)) {
        $meetingCounts =  $meetingResults['num_rows'];
        $meetingUpdateFFA = new Meeting($countries[0], 'ffa');
        $meetingUpdateFFAResults = $meetingUpdateFFA->updateRNAEtlSync($meetingResults['last_insert_id'], $meetingResults['num_rows']);
    } else {
        $meetingCounts = $meetingResults;
    }
    /** End of Farmer Meeting **/


    /** Retailer Visit **/
    $retailer = new Retailer($countries[0], 'analytical');
    $retailerResults = $retailer->getStaging();

    if (is_array($retailerResults)) {
        $retailerCounts =  $retailerResults['num_rows'];
    
        $retailerUpdateFFA = new Retailer($countries[0], 'ffa');
        $retailerUpdateFFAResults = $retailerUpdateFFA->updateRNAEtlSync($retailerResults['last_insert_id'], $retailerResults['num_rows']);
    } else {
        $retailerCounts = $retailerResults;
    }

    /** End of Retailer Visit **/
    
    /** Users **/
    $users_from_ffa = new User($countries[0], 'ffa');
    $userLastSynced = $users_from_ffa->__checkRecordFFASync();
    $last_insert_id = $userLastSynced ? $userLastSynced['last_insert_id'] : '';

    $users = new User($countries[0], 'analytical');
    $usersEtl = $users->getStagingETL($last_insert_id);

    if (isset($usersEtl['num_rows'])) {
        $usersCounts = $usersEtl['num_rows'];
    } else {
        $usersCounts = $usersEtl['message'];
    }

    // update ffa tbl_rna_etl_sync table
    $user_to_ffa = new User($countries[0], 'ffa');
    $user_ffa_etl_sync = $user_to_ffa->updateTblRnaEtlSyncFFA($usersEtl);
    /** End Users **/
    
    
    /** Region **/
    $region_from_ffa = new Region($countries[0], 'ffa');
    $regionLastSynced = $region_from_ffa->__checkRecordFFASync();
    $last_insert_id = $regionLastSynced ? $regionLastSynced['last_insert_id'] : '';

    $region = new Region($countries[0], 'analytical');
    $regionEtl = $region->getStagingETL($last_insert_id);
    
    if (isset($regionEtl['num_rows'])) {
        $regionCounts = $regionEtl['num_rows'];
    } else {
        $regionCounts = $regionEtl['message'];
    }

    // update ffa tbl_rna_etl_sync table
    $region_to_ffa = new Region($countries[0], 'ffa');
    $region_ffa_etl_sync = $region_to_ffa->updateTblRnaEtlSyncFFA($regionEtl); 
    /** End Region **/


    /** Territory **/

    //check ffa last synced date and id for user record
    $territories_from_ffa = new Territory($countries[0], 'ffa');
    $territoryLastSynced = $territories_from_ffa->__checkRecordFFASync();
    $last_insert_id = $territoryLastSynced ? $territoryLastSynced['last_insert_id'] : '';

    //territories
    $territories = new Territory($countries[0], 'analytical');
    $territoriesEtl = $territories->getStaging($last_insert_id);

    if (isset($territoriesEtl['num_rows'])) {
        $territoryCounts = $territoriesEtl['num_rows'];
    } else {
        $territoryCounts = $territoriesEtl['message'];
    }

    // update ffa tbl_rna_etl_sync table
    $territory_to_ffa = new Territory($countries[0], 'ffa');
    $territory_ffa_etl_sync = $territory_to_ffa->updateTblRnaEtlSyncFFA($territoriesEtl);
    
    /** End of Territory **/

    /** Zone **/
    //check ffa last synced date and id for user record
    $zones_from_ffa = new Zone($countries[0], 'ffa');
    $zoneLastSynced = $zones_from_ffa->__checkRecordFFASync();
    $last_insert_id = $zoneLastSynced ? $zoneLastSynced['last_insert_id'] : '';
    //no last_insert_id
    
    $zones = new Zone($countries[0], 'analytical');
    $zonesEtlMessage = $zones->getStagingETL($last_insert_id);

    if (isset($zonesEtlMessage['num_rows'])) {
        $zoneCounts = $zonesEtlMessage['num_rows'];
    } else {
        $zoneCounts = $zonesEtlMessage['message'];
    }

    // update ffa tbl_rna_etl_sync table
    $zone_to_ffa = new Zone($countries[0], 'ffa');
    $zone_ffa_etl_sync = $zone_to_ffa->updateTblRnaEtlSyncFFA($zonesEtlMessage);
    /** End of Zone **/

    /** Portal Settings **/
    //check ffa last synced date and id for portal settings record
    $portal_setting_from_ffa = new PortalSetting($countries[0], 'ffa');
    $portalSettingLastSynced = $portal_setting_from_ffa->__checkRecordFFASync();
    $last_insert_id = $portalSettingLastSynced ? $portalSettingLastSynced['last_insert_id'] : '';

    //portal settings
    $portalSetting = new PortalSetting($countries[0], 'analytical', $last_insert_id);
    $portalSettingEtl = $portalSetting->getStagingETL();

    if (is_array($portalSettingEtl)) {
        $portalSettingsCounts =  $portalSettingEtl['num_rows'];

            // update ffa tbl_rna_etl_sync table
        $portal_setting_to_ffa = new PortalSetting($countries[0], 'ffa');
        $portal_ffa_etl_sync = $portal_setting_to_ffa->updateTblRnaEtlSyncFFA($portalSettingEtl);
    } else {
        $portalSettingsCounts = $portalSettingEtl;
    }

     
    /** End of Portal **/

    /** Teams **/

    $teams = new Teams($countries[0], 'analytical');
	$teamsResults = $teams->getStaging();
    
    if (is_array($teamsResults)) {
        $teamsCounts =  $teamsResults['num_rows'];
        $updateFFA = new Teams($countries[0], 'ffa');
        $updateFFAResults = $updateFFA->updateRNAEtlSync($teamsResults['last_insert_id'], $teamsResults['num_rows']);
    } else {
        $teamsCounts = $teamsResults;
    }

    /** End of Teams **/

    /** Complaints **/

    $complaint = new Complaint($countries[0], 'analytical');
	$complaintResults = $complaint->getStaging();
    
    if (is_array($complaintResults)) {
        $complaintCounts =  $complaintResults['num_rows'];
        $updateFFA = new Complaint($countries[0], 'ffa');
        $updateFFAResults = $updateFFA->updateRNAEtlSync($complaintResults['last_insert_id'], $complaintResults['num_rows']);
    } else {
        $complaintCounts = $complaintResults;
    }

    /** End of Complaints **/
    
    /** Crops **/

    $crops = new Crop($countries[0], 'analytical');
	$cropsResults = $crops->getStaging();
    
    if (is_array($cropsResults)) {
        $cropsCounts =  $cropsResults['num_rows'];
        $updateFFA = new Crop($countries[0], 'ffa');
        $updateFFAResults = $updateFFA->updateRNAEtlSync($cropsResults['last_insert_id'], $cropsResults['num_rows']);
    } else {
        $cropsCounts = $cropsResults;
    }

    /** End of Crops **/

    /** Products **/

     $products = new Product($countries[0], 'analytical');
     $productsResults = $products->getStaging();
     
     if (is_array($productsResults)) {
         $productsCounts =  $productsResults['num_rows'];
         $updateFFA = new Product($countries[0], 'ffa');
         $updateFFAResults = $updateFFA->updateRNAEtlSync($productsResults['last_insert_id'], $productsResults['num_rows']);
     } else {
         $productsCounts = $cropsResults;
     }
 
     /** End of Products **/
    
    /** Target Activity **/

     $targetActivity = new TargetActivity($countries[0], 'analytical');
     $targetActivityResults = $targetActivity->getStagingETL();
     
     if (is_array($targetActivityResults)) {
         $targetActivityCounts =  $targetActivityResults['num_rows'];
         $updateFFA = new Product($countries[0], 'ffa');
         $updateFFAResults = $updateFFA->updateRNAEtlSync($targetActivityResults['last_insert_id'], $targetActivityResults['num_rows']);
     } else {
         $targetActivityCounts = $targetActivityResults;
     }
    
    /** End of Target Activity **/

    $countryName = $countries[0]['country_name'];
    $txt =
    "Country: $countryName\n" .
    "Demo Counts: $demoCounts\n" .
    "Meeting Counts: $meetingCounts\n" .
    "Retailer Counts: $retailerCounts\n" .
    "User Counts: $usersCounts\n" .
    "Region Counts: $regionCounts\n" .
    "Zone Counts: $zoneCounts\n" .
    "Territory Counts: $territoryCounts\n" .
    "Teams Counts: $teamsCounts\n".
    "Portal Settings Counts: $portalSettingsCounts\n".
    "Complaint Counts: $complaintCounts\n".
    "Crops Counts: $cropsCounts\n".
    "Products Counts: $productsCounts\n".
    "Target Activity Counts: $targetActivityCounts\n\n";

    echo $txt;

    echo Logs::success("ETL Vietnam Process Completed: " . date('Y-m-d H:i:s') . "\n");
}

run();