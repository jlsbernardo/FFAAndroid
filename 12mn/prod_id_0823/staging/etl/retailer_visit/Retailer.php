<?php

require_once(dirname(__FILE__, 3) . '/src/RNA/Database/DB.php');

/**
 * This script is for generating .sql and .txt file for staging (for all countries)
 * That will be imported to Analytical Database using any database tools ie. MySQL Workbench
 * 
 * @return \Logs
 */

class Retailer extends DB
{

    protected $ffaTable = 'tbl_ffa_retailer';

    protected $stagingTable = '';

    protected $reportTable = 'retailer_visit_reports';

    protected $ffaGps = 'tbl_ffa_images';

    protected $country = '';

    protected $connection;

    protected $ffaSyncTable = 'tbl_rna_etl_sync';

    protected $schemaName = 'analytical';

    public function __construct($country = '', $connection = 'ffa')
    {
        $this->country = $country;
        if ($country != '') {
            $this->stagingTable = 'staging_' . $country['short_name'];
        }

        $this->connection = $connection;
        parent::__construct();
    }

    private function __checkDeleted($ffa_id)
    {

        $sql = "SELECT ffa_id FROM tbl_deleted_activities WHERE module = '$this->reportTable' AND 'ffa_id' = $ffa_id limit 1";
        $result = $this->exec_query($sql);

        if ($result->num_rows > 0) {
            // return true;
            $row = $result->fetch_assoc();
            if (date('Y-m-d H:i') >= $row['deleted_at']) {
                return true;
            }
        }
        
        return false;
    }

    /**
     * Get Data from FFA Database
     *
     * @return string | array
     */
    public function getDataFromFFA()
    {
        echo Logs::success("12MN ID RETAILER getDataFromFFA Process Starts: " . date('Y-m-d H:i:s') . "\n");
        $checkLastRecord = $this->__checkLastRecordFFASync();
        $lastInserted = ($checkLastRecord) ? $checkLastRecord['last_insert_id'] : null;
        $only2022_data = strtotime('2022-04-01 00:00:00');
        $sql = "SELECT
            $this->ffaTable.id,
            UNIX_TIMESTAMP(CONVERT_TZ(FROM_UNIXTIME($this->ffaTable.create_on), '".UTC_TIMEZONE."', '".CURRENT_TIMEZONE."')) as create_on,
            created_by,
            UNIX_TIMESTAMP(CONVERT_TZ(FROM_UNIXTIME(update_on), '".UTC_TIMEZONE."', '".CURRENT_TIMEZONE."')) as update_on,
            update_by,
            UNIX_TIMESTAMP(CONVERT_TZ(FROM_UNIXTIME(plan_on), '".UTC_TIMEZONE."', '".CURRENT_TIMEZONE."')) as plan_on,
            plan_by,
            UNIX_TIMESTAMP(CONVERT_TZ(FROM_UNIXTIME(approved_on), '".UTC_TIMEZONE."', '".CURRENT_TIMEZONE."')) as approved_on,
            approved_by,
            UNIX_TIMESTAMP(CONVERT_TZ(FROM_UNIXTIME(closed_on), '".UTC_TIMEZONE."', '".CURRENT_TIMEZONE."')) as closed_on,
            closed_by,
            date,
            team,
            host_name,
            host_phone,
            month,
            $this->ffaTable.status,
            temp_execute,
            products,
            territory,
            participant_list,
            supervisor,
            marked_by,
            marked_on,
            crop,
            tmp.lat as imglat,
            tmp.lng as imglng
        FROM
            $this->ffaTable
        LEFT JOIN
        (
            SELECT s.* FROM $this->ffaGps AS s ORDER BY s.id DESC
        ) AS tmp ON $this->ffaTable.id = tmp.ref_id
        AND tmp.category='retailer'
        WHERE
            UNIX_TIMESTAMP(CONVERT_TZ(FROM_UNIXTIME($this->ffaTable.create_on), '".UTC_TIMEZONE."', '".CURRENT_TIMEZONE."'))>=$only2022_data
        GROUP BY 
            $this->ffaTable.id
        order by
            $this->ffaTable.create_on
        desc";

        $retailerInsertQuery = "";
        $result = $this->exec_query($sql);
        $data = [];
        $country = $this->country['country_name'];
        if ($result->num_rows > 0) {
            while ($row = $result->fetch_assoc()) {

                $deleted = $this->__checkDeleted($row['id']);
                $territory = $row['territory'];

                $zoneRegion = $this->getRetailerZoneRegion($territory);
                if($row['closed_by']!==null && $row['closed_by']==$row['created_by']){
                    $supervisorId = $territory!==null ? $this->getSupervisor($row['id'],intval($territory),$row['team']) : null;
                }else{
                    $supervisorId = $row['created_by'];
                }

                $temp_execute_decode = json_decode($row['temp_execute'], true);

                $temp_execute = (isset($temp_execute_decode) && $temp_execute_decode['client']['time']) ? $temp_execute_decode['client']['time'] : null;

                $objVN = new vn_charset_conversion();
                $hn = preg_replace("/[^a-zA-Z0-9]+/", "", html_entity_decode($row['host_name'], ENT_QUOTES));
                $hostname = (strtolower($country)=='vietnam') ? $objVN->convert($hn) : $hn;
                $team = (strtolower($country)=='vietnam') ? $objVN->convert($row['team']) : $row['team'];
                $products = (strtolower($country)=='vietnam') ? $objVN->convert($row['products']) : $row['products'];
                $participants = preg_replace('~[\x00\x0A\x0D\x1A\x22\x27\x5C]~u', '\\\$0', str_replace("'", "", $row['participant_list']));
                $participant_list = (strtolower($country)=='vietnam') ? $objVN->convert($participants) : $participants;

                $retailerRNAFields = array(
                    'ffa_id'        => $row['id'],
                    'deleted'       => $deleted ? '1' : '0',
                    'report_table'  => $this->reportTable,
                    'create_on'     => ($row['create_on']) ? $row['create_on'] : 0,
                    // 'created_by'    => $row['created_by'],
                    'created_by'    =>$supervisorId,
                    'update_by'     => $row['update_by'],
                    'plan_by'       => $row['plan_by'],
                    'approved_by'   => $row['approved_by'],
                    'closed_by'     => $row['closed_by'],
                    'team'          => (strtolower($country)=='thailand') ? $team : trim($team),
                    'host_name'     => $hostname,
                    'host_phone'    => $row['host_phone'],
                    'month'         => $row['month'],
                    'status'        => $row['status'],
                    'temp_execute'  => $temp_execute,
                    'products'      => $products,
                    'territory'     => $row['territory'],
                    'zone'          => $zoneRegion['zone'],
                    'region'        => $zoneRegion['region'],
                    'participant_list' => $participant_list,
                    'supervisor'    => $row['supervisor'],
                    'marked_by'     => !is_null($row['marked_by']) ? $row['marked_by'] : 0,
                    'crop_id'       => ($row['crop']) ? $row['crop'] : 0,
                    'lat'           => ($row['imglat']) ? $row['imglat'] : 0,
                    'lng'           => ($row['imglng']) ? $row['imglng'] : 0,
                );

                if (!empty($row['update_on']) && !is_null($row['update_on'])) {
                    $retailerRNAFields['update_on'] = $row['update_on'];
                }

                if (!empty($row['closed_on']) && !is_null($row['closed_on'])) {
                    $retailerRNAFields['closed_on'] = $row['closed_on'];
                }
                
                if (!empty($row['approved_on']) && !is_null($row['approved_on'])) {
                    $retailerRNAFields['approved_on'] = $row['approved_on'];
                }

                if (!empty($row['execute_on']) && !is_null($row['execute_on'])) {
                    $retailerRNAFields['execute_on'] = $row['execute_on'];
                }

                if (!empty($row['plan_on']) && !is_null($row['plan_on'])) {
                    $retailerRNAFields['plan_on'] = $row['plan_on'];
                }

                if (!empty($row['date']) && !is_null($row['date'])) {
                    $retailerRNAFields['date'] = $row['date'];
                }

                if (!empty($row['marked_on']) && !is_null($row['marked_on'])) {
                    $retailerRNAFields['marked_on'] = $row['marked_on'];
                }

                // $strColumns = implode(', ', array_keys($retailerRNAFields));
                // $strValues =  " '" . implode("', '", array_values($retailerRNAFields)) . "' ";
                // $retailerInsertQuery .= "INSERT INTO $this->stagingTable ({$strColumns}) VALUES ({$strValues}); \n";

                $data[] = $retailerRNAFields;
            }
            echo Logs::success("12MN ID RETAILER getDataFromFFA Process End: " . date('Y-m-d H:i:s') . "\n");
            return [
                'data' => $data,
                'last_inserted' => $lastInserted
            ];

        } else {
            echo Logs::success("12MN ID RETAILER getDataFromFFA Process End: " . date('Y-m-d H:i:s') . "\n");
            $message = "No Retailer Visit Records to sync";
            return [
                'data' => $message
            ];
        }
        
    }

    /**
     * Insert to staging table
     *
     * @param $data
     * @return array
     */
    public function insertIntoStaging($data, $lastInserted = null)
    {
        echo Logs::success("12MN ID RETAILER insertIntoStaging Process Starts: " . date('Y-m-d H:i:s') . "\n");
        $count = 0;
        $objVN = new vn_charset_conversion();
        $country = $this->country['country_name'];
        foreach ($data as $retailerRNAFields) {

            if (!empty($retailerRNAFields['create_on']) && !is_null($retailerRNAFields['create_on']) || !empty($retailerRNAFields['update_on']) && !is_null($retailerRNAFields['update_on'])) {
                // if (date('Y-m-d') == convert_to_datetime($retailerRNAFields['create_on'], 'Y-m-d') || date('Y-m-d') == convert_to_datetime($retailerRNAFields['update_on'], 'Y-m-d')) {
                    $results = $this->__checkRecordStaging($retailerRNAFields);

                    if (sqlsrv_num_rows($results) < 1) {
                            $strColumns = implode(', ', array_keys($retailerRNAFields));
                            $strValues =  " '" . implode("', '", array_values($retailerRNAFields)) . "' ";
                            $retailerInsertQuery = "INSERT INTO [$this->schemaName].[$this->stagingTable] ({$strColumns}) VALUES ({$strValues});";

                            $result = $this->exec_query($retailerInsertQuery);

                            if ($result) {
                                $count += 1;
                            }
                    } else {
                        $plan_on = (isset($retailerRNAFields['plan_on'])) ? 'plan_on = ' . "'{$retailerRNAFields['plan_on']}'," : null;
                        $approved_on = (isset($retailerRNAFields['approved_on'])) ? 'approved_on =' . "'{$retailerRNAFields['approved_on']}'," : null;
                        $closed_on = (isset($retailerRNAFields['closed_on'])) ? 'closed_on =' . "'{$retailerRNAFields['closed_on']}'," : null;
                        $date = isset($retailerRNAFields['date']) ? 'date =' . "'{$retailerRNAFields['date']}'," : null;

                        $ffaId = $retailerRNAFields['ffa_id'];
                        $participantList = preg_replace('~[\x00\x0A\x0D\x1A\x22\x27\x5C]~u', '\\\$0', $retailerRNAFields['participant_list']);
                        $temp_execute =  $retailerRNAFields['temp_execute'];
                        $participantList = preg_replace('~[\x00\x0A\x0D\x1A\x22\x27\x5C]~u', '\\\$0', $retailerRNAFields['participant_list']);

                        $markedBy = !is_null($retailerRNAFields['marked_by']) ? $retailerRNAFields['marked_by'] : 0;
                        $markedOn = isset($retailerRNAFields['marked_on']) ? 'marked_on =' . "'{$retailerRNAFields['marked_on']}'," : null;
                        $team = (strtolower($country)=='vietnam') ? $objVN->convert( $retailerRNAFields['team']   ) : $retailerRNAFields['team'];
                        $team = (strtolower($country)=='thailand') ? $team : trim($team);

                        $RETAILERUpdateQuery = "
                        UPDATE [$this->schemaName].[$this->stagingTable]
                        SET 
                            update_on= '{$retailerRNAFields['update_on']}',
                            update_by = '{$retailerRNAFields['update_by']}',
                            {$plan_on}
                            plan_by   = '{$retailerRNAFields['plan_by']}',
                            {$approved_on}
                            approved_by = '{$retailerRNAFields['approved_by']}',
                            {$closed_on}
                            closed_by      = '{$retailerRNAFields['closed_by']}',
                            {$date}
                            team      = '{$team}',
                            deleted = {$retailerRNAFields['deleted']},
                            host_name      = '{$retailerRNAFields['host_name']}',
                            host_phone      = '{$retailerRNAFields['host_phone']}',
                            month      = '{$retailerRNAFields['month']}',
                            products      = '{$retailerRNAFields['products']}',
                            territory      = '{$retailerRNAFields['territory']}',
                            zone      = '{$retailerRNAFields['zone']}',
                            region      = '{$retailerRNAFields['region']}',
                            participant_list = '{$participantList}',
                            supervisor = '{$retailerRNAFields['supervisor']}',
                            marked_by = '{$markedBy}',
                            {$markedOn}
                            temp_execute = '{$temp_execute}',
                            crop_id      = '{$retailerRNAFields['crop_id']}',
                            lat      = '{$retailerRNAFields['lat']}',
                            lng      = '{$retailerRNAFields['lng']}'
                        WHERE ffa_id = '$ffaId' AND report_table = '$this->reportTable';";

                        $result =  $this->exec_query($RETAILERUpdateQuery);
                        if ($result) {
                            $count += sqlsrv_rows_affected($result);
                        }
                    }
                // }
            }
        }
        echo Logs::success("12MN ID RETAILER insertIntoStaging Process End: " . date('Y-m-d H:i:s') . "\n");
        return [
            'count' => $count
        ];
    }

    private function __checkLastRecordFFASync()
    {
        
        $sql = "SELECT id, last_insert_id, last_synced_date FROM $this->ffaSyncTable WHERE module = '$this->reportTable' AND action_name = 'create' ORDER BY id desc LIMIT 1";
        $results = $this->exec_query($sql);
        
        if ($results->num_rows > 0) {
            $row = $results->fetch_assoc();
            return $row;
        }

        return false;
    }

    /**
     * Checking record in staging table
     *
     * @param $data
     * @return object | array
     */
    private function __checkRecordStaging($data)
    {
        $ffaId = $data['ffa_id'];
        $sql = "SELECT TOP 1 ffa_id, report_table FROM [$this->schemaName].[$this->stagingTable] WHERE report_table = '$this->reportTable' AND ffa_id = '$ffaId' ORDER BY id desc";
        $res = $this->exec_query($sql);
        return $res;
    }

    private function getRetailerZoneRegion($territory)
    {
        
        $zoneSQL = "SELECT
            id,
            level
        FROM tbl_area_structure
        WHERE id = $territory LIMIT 1";

        $zone = $this->exec_query($zoneSQL);
        
        $zoneId = null;
        if ($zone->num_rows > 0) {
            $row = $zone->fetch_assoc();
            $zoneId = $row['level'];
            $regionId = $this->getRetailerRegion($zoneId);
            
             return [
                'zone'  => $zoneId,
                'region'    => $regionId
            ];
        }
        
    }

    private function getRetailerRegion($zoneId)
    {   
        
        $regionSQL = "SELECT
            id,
            level
        FROM tbl_area_structure
        WHERE id = $zoneId LIMIT 1";

        $region = $this->exec_query($regionSQL);

        if ($region->num_rows > 0) {
            $row = $region->fetch_assoc();
            $regionId = $row['level'];
            return $regionId;
        }
        
    }
    private function getSupervisor($ffa, $territory,$team)
    {   
        echo Logs::success("12MN ID RETAILER getSupervisor Process Starts: " . date('Y-m-d H:i:s') . "\n");
        $supSQL =  "SELECT id, uterritory FROM users
        WHERE  active=1 AND team='$team' AND company='ZM' AND (uterritory<>'N;' ) order by id asc";
        
        $sup = $this->exec_query($supSQL);

        $supId = null;
        if ($sup->num_rows > 0) {
            $row = $sup->fetch_all();
            foreach($row as $s){
                    $territories = json_decode($s[1]);
                    if (in_array($territory, (array) $territories)) {
                         return $s[0];
                    }
            }
        }
        echo Logs::success("12MN ID RETAILER getSupervisor Process End: " . date('Y-m-d H:i:s') . "\n");
        return $supId;
    }
}
