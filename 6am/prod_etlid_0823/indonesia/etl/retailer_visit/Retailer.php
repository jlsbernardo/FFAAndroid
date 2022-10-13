<?php

require_once(dirname(__FILE__, 3) . '/src/RNA/Database/DB.php');

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
     * Get Data From Staging Table
     *
     * @return array | string
     */
    public function getStaging()
    {
        $retailerSql = "SELECT
        id,
        ffa_id,
        deleted,
        create_on,
        created_by,
        update_on,
        update_by,
        plan_on,
        plan_by,
        approved_on,    
        approved_by,
        closed_on,
        closed_by,
        date,
        team,
        host_name,
        host_phone,
        month,
        status,
        temp_execute,
        products,
        territory,
        zone,
        region,
        participant_list,
        supervisor,
        marked_by,
        marked_on,
        crop_id,
        lat,
        lng
        FROM
            [$this->schemaName].[$this->stagingTable]
        WHERE
            report_table = '$this->reportTable'
        order by
            create_on
        desc";

        $retailerInsertQuery = "";
        $result = $this->exec_query($retailerSql);
        $retailerRNAFields = [];
        $duplicate = [];
        
        if (sqlsrv_num_rows($result) > 0) {
            while ($row = sqlsrv_fetch_array($result)) {

                if ($row['status'] == 'close') {
                    if (!is_null($row['closed_on']) && $row['closed_on'] != '') {
                        $duplicate[$row['id']] = [
                            'host_name' => preg_replace("/[^a-zA-Z0-9]+/", "", html_entity_decode($row['host_name'], ENT_QUOTES)),
                            'month' => convert_to_datetime( $row['closed_on'], 'Y-m-d')
                        ];
                    }
                }

                $retailerRNAFields[] = array(
                    'ffa_id' => $row['ffa_id'],
                    'deleted' => $row['deleted'],
                    'create_on' => convert_to_datetime($row['create_on']),
                    'created_by' => $row['created_by'],
                    'update_on' => ($row['update_on'] != '0' && $row['update_on'] != null) ? convert_to_datetime($row['update_on']) : '1970-01-01 00:00:00',
                    'update_by' => $row['update_by'],
                    'plan_on'   => $row['plan_on'],
                    'plan_by'   => $row['plan_by'],
                    'approved_on'  => $row['approved_on'],
                    'approved_by'   => $row['approved_by'],
                    'closed_on'  => $row['closed_on'],
                    'closed_by'   => $row['closed_by'],
                    'date'  => ($row['date']),
                    'month' => $row['month'],
                    'team'  => $row['team'],
                    'host_name' => preg_replace("/[^a-zA-Z0-9]+/", "", html_entity_decode($row['host_name'], ENT_QUOTES)),
                    'host_phone'    => $row['host_phone'],
                    'status' => $row['status'],
                    // 'month' => $row['month'],
                    'month' => date("M-Y",$row['create_on'] ),
                    'products' => $row['products'],
                    'territory_id'   => $row['territory'],
                    'zone_id'   => $row['zone'],
                    'region_id'   => $row['region'],
                    'country'   => $this->country['country_name'],
                    'client_execute_submit_date_time'   => $row['temp_execute'] ? $row['temp_execute'] : null,
                    'participant_list'  => $row['participant_list'],
                    'supervisor'    => $row['supervisor'],
                    'marked_by' => !is_null($row['marked_by']) ? $row['marked_by'] : 0,
                    'marked_on' => $row['marked_on'],
                    'crop_id'   => $row['crop_id'],
                    'lat'     => (isset($row['lat']) && $row['lat']!==null) ? $row['lat'] : 0,
                    'lng'     => (isset($row['lng']) && $row['lng']!==null) ? $row['lng'] : 0
                );
            }

            $arr_uniq = array_unique($duplicate, SORT_REGULAR);

            // get the duplicated data
            $diffCellUniq = array_diff_key($duplicate, $arr_uniq);
            
            $incorrectData = $this->getPortalSettingsKey('phone-length-restriction');
            $phoneLength = isset($incorrectData['value']) ? explode('|', $incorrectData['value']):[];
            $phonePrefix = isset($this->getPortalSettingsKey('phone-prefix')['value'])? $this->getPortalSettingsKey('phone-prefix')['value']:'';
            
            $offBusinessHours = $this->getOffBusinessHours();
            $minRetailerParticipants = $this->getPortalSettingsKey('minimum-retailer-participants');
            
            $countRetailerAffectedRows = 0;
            foreach ($retailerRNAFields as $key => $retailerRNAField) {
                
                $isDuplicate = 0;
                $isOffBusinessHours = 0;
                $uniqHost = '';
                $uniqMonth = '';
                $isInvalidParticipantCount = 0;
                
                if ($retailerRNAField['host_phone'] != '' && !is_null($retailerRNAField['host_phone'])) {
                    $hostPhone = str_replace($phonePrefix, '', $retailerRNAField['host_phone']);
                    if( is_array($phoneLength) && count($phoneLength)>0 ){
                        if (strlen($hostPhone) < $phoneLength[0] || strlen($hostPhone) > $phoneLength[1]) {
                            $retailerRNAField['incorrect_data'] = 1;
                        }
                    }
                }

                if ($retailerRNAField['status'] != 'close') {
                    $retailerRNAField['incomplete_activity'] = 1;
                }
                
                if (!empty($retailerRNAField['plan_on']) && $retailerRNAField['plan_on'] != '0' && !is_null($retailerRNAField['plan_on'])) {
                    $retailerRNAField['plan_on'] = convert_to_datetime($retailerRNAField['plan_on']);
                } else {
                    unset($retailerRNAField['plan_on']);
                }
                
                if (isset($retailerRNAField['approved_on']) && !empty($retailerRNAField['approved_on']) && $retailerRNAField['approved_on'] != '0' && !is_null($retailerRNAField['approved_on'])) {
                    $retailerRNAField['approved_on'] = convert_to_datetime($retailerRNAField['approved_on']);
                } else {
                    unset($retailerRNAField['approved_on']);
                }

                if (isset($retailerRNAField['closed_on']) && !empty($retailerRNAField['closed_on']) && $retailerRNAField['closed_on'] != '0' && !is_null($retailerRNAField['closed_on'])) {
                    $retailerRNAField['closed_on'] = convert_to_datetime($retailerRNAField['closed_on']);
                } else {
                    unset($retailerRNAField['closed_on']);
                }

                if (isset($retailerRNAField['date']) && !empty($retailerRNAField['date']) && !is_null($retailerRNAField['date']) && $retailerRNAField['date'] != '1970-01-01 00:00:00') {
                    $retailerRNAField['date'] = $retailerRNAField['date'];
                } else {
                    unset($retailerRNAField['date']);
                }
                if (isset($retailerRNAField['client_execute_submit_date_time']) && !is_null($retailerRNAField['client_execute_submit_date_time'])) {
                    $retailerRNAField['client_execute_submit_date_time'] = convert_to_datetime($retailerRNAField['client_execute_submit_date_time'], 'Y-m-d H:i:s');

                    $clientExecute = date('H:i', strtotime($retailerRNAField['client_execute_submit_date_time']));
                    $isRange = ($offBusinessHours['start']!==null && $offBusinessHours['end']!==null && $clientExecute!==null) ? time_in_range($offBusinessHours['start'], $offBusinessHours['end'], $clientExecute) : false;
                
                    if (!$isRange) {
                        $isOffBusinessHours = 1;
                    }
                    
                    $retailerRNAField['off_business_hours'] = $isOffBusinessHours;
                    
                } else {
                    unset($retailerRNAField['client_execute_submit_date_time']);
                }

                foreach ($diffCellUniq as $uniq) {
                    $uniqHost = $uniq['host_name'];
                    $uniqMonth = $uniq['month'];

                    if (isset($retailerRNAField['closed_on']) && !is_null($uniqMonth)) {
                        if (
                            $retailerRNAField['host_name'] == $uniqHost &&
                            date('Y-m-d', strtotime($retailerRNAField['closed_on'])) == $uniqMonth
                        ) {
                            $isDuplicate = 1;
                        }
                    }
                }

                if (isset($retailerRNAField['participant_list']) && !is_null($retailerRNAField['participant_list']) && $retailerRNAField['participant_list'] !== '') {
                    $participantList = json_decode($retailerRNAField['participant_list'], TRUE);
                    if (is_array($participantList)) {
                        if (count($participantList) < $minRetailerParticipants['value']) {
                            $isInvalidParticipantCount = 1;
                        }
                    }
                }

                if (isset($retailerRNAField['marked_on']) && !is_null($retailerRNAField['marked_on'])) {
                    $retailerRNAField['marked_on'] = convert_to_datetime($retailerRNAField['marked_on']);
                } else {
                    unset($retailerRNAField['marked_on']);
                }

                $lastInserted = $retailerRNAField['ffa_id'];
                $check_record = $this->__checkRetailerRecord($lastInserted);
                
                $retailerRNAField['duplicate_data'] = $isDuplicate;
                $retailerRNAField['invalid_participant_count'] = 0;
                $retailerRNAField['invalid_participant_count'] = $isInvalidParticipantCount;
                
                $removeSpecificColumns = array_diff_key($retailerRNAField, array_flip(["participant_list"]));
                
                if (!is_array($check_record)) {
                    $strColumns = implode(', ', array_keys($removeSpecificColumns));
                    $strValues =  " '" . implode("', '", array_values($removeSpecificColumns)) . "' ";
                    $retailerInsertQuery = "INSERT INTO [$this->schemaName].[$this->reportTable] ({$strColumns}) VALUES ({$strValues});";

                    $result = $this->exec_query($retailerInsertQuery);
                    if ($result) {
                        $countRetailerAffectedRows += 1;
                    }

                    $lastInserted = $retailerRNAField['ffa_id'];

                } else {
                    $country = $this->country['country_name'];
                    $plan_on = (isset($retailerRNAField['plan_on'])) ? 'plan_on = '."'{$retailerRNAField['plan_on']}'," : null;
                    $approved_on = (isset($retailerRNAField['approved_on'])) ? 'approved_on ='."'{$retailerRNAField['approved_on']}'," : null;
                    $closed_on = (isset($retailerRNAField['closed_on'])) ? 'closed_on ='."'{$retailerRNAField['closed_on']}'," : null;
                    $date = isset($retailerRNAField['date']) ? 'date ='."'{$retailerRNAField['date']}'," : null;
                    $client_execute_submit_date_time = isset($retailerRNAField['client_execute_submit_date_time']) ? 'client_execute_submit_date_time ='."'{$retailerRNAField['client_execute_submit_date_time']}'," : null;
                    $markedBy = !is_null($retailerRNAField['marked_by']) ? $retailerRNAField['marked_by'] : 0;
                    $markedOn = isset($retailerRNAField['marked_on']) ? 'marked_on =' . "'{$retailerRNAField['marked_on']}'," : null;
                    $crop_id = isset($retailerRNAField['crop_id']) ? $retailerRNAField['crop_id'] : 0;
                    $lat = (isset($retailerRNAField['lat']) && $retailerRNAField['lat']!==null) ? $retailerRNAField['lat'] : 0;
                    $lng = (isset($retailerRNAField['lng']) && $retailerRNAField['lng']!==null) ? $retailerRNAField['lng'] : 0;

                    $demoUpdateQuery = "
                        UPDATE [$this->schemaName].[$this->reportTable]
                        SET 
                            update_on= '{$retailerRNAField['update_on']}',
                            update_by = '{$retailerRNAField['update_by']}',
                            deleted = '{$retailerRNAField['deleted']}',
                            {$plan_on}
                            plan_by   = '{$retailerRNAField['plan_by']}',
                            {$approved_on}
                            approved_by = '{$retailerRNAField['approved_by']}',
                            {$closed_on}
                            closed_by      = '{$retailerRNAField['closed_by']}',
                            {$date}
                            team      = '{$retailerRNAField['team']}',
                            host_name      = '{$retailerRNAField['host_name']}',
                            host_phone      = '{$retailerRNAField['host_phone']}',
                            month      = '{$retailerRNAField['month']}',
                            status = '{$retailerRNAField['status']}',
                            products      = '{$retailerRNAField['products']}',
                            territory_id      = '{$retailerRNAField['territory_id']}',
                            zone_id      = '{$retailerRNAField['zone_id']}',
                            country      = '{$this->country['country_name']}',
                            {$client_execute_submit_date_time}
                            supervisor = '{$retailerRNAField['supervisor']}',
                            marked_by = '{$markedBy}',
                            {$markedOn}
                            region_id      = '{$retailerRNAField['region_id']}',
                            crop_id      = '{$crop_id}',
                            lat      = '{$lat}',
                            lng      = '{$lng}'
                        WHERE ffa_id = '$lastInserted'
                        AND country = '$country';";

                    $result =  $this->exec_query($demoUpdateQuery);
                    if ($result) {
                        $countRetailerAffectedRows += sqlsrv_rows_affected($result);
                    }
                }
            }
            
            return [
                'num_rows'    => $countRetailerAffectedRows,
                'last_insert_id'  => $lastInserted
            ];

        } else {
            $message = "No Retailer Reports Records to sync";
            return $message;
        }
    }
    
    /**
     * Update tbl_rna_etl_sync in FFA Database
     *
     * @param $lastInserted
     * @param $count
     * 
     */
    public function updateRNAEtlSync($lastInserted, $count) 
    {
        $currentDateTime = date('Y-m-d H:i:s');
        $checkRecords = $this->__checkRecordFFASync($lastInserted);
        $action = !$checkRecords ? 'create' : 'update';

        if ($action == 'create') {
            $insert = "INSERT INTO $this->ffaSyncTable (`action_name`, `module`, `last_synced_date`, `status`, `record_count`, `last_insert_id`) 
            VALUES ('create', '$this->reportTable', '$currentDateTime' , 'active', '$count', '$lastInserted');";
            $this->insert_query($insert);
        } else {
            $insert = "INSERT INTO $this->ffaSyncTable (`action_name`, `module`, `last_synced_date`, `status`, `record_count`, `last_insert_id`) 
            VALUES ('update', '$this->reportTable', '$currentDateTime' , 'active', '$count', '$lastInserted');";
            $this->insert_query($insert);
        }
    }

    // Check the user record on ffa.tbl_rna_etl_sync table
    public function __checkRecordFFASync($lastInserted)
    {
        $where = (!is_null($lastInserted) && $lastInserted != '') ? 'AND last_insert_id = ' . $lastInserted : '';
        $sql = "SELECT id FROM $this->ffaSyncTable WHERE module = '$this->reportTable' $where ORDER BY id DESC  LIMIT 1 ";
        
        $results = $this->exec_query($sql);
        if ($results->num_rows > 0) {
            $row = $results->fetch_assoc();
            return $row;
        }
        
        return false;
    }

    /**
     * Check Retailer Visit Record
     *
     * @param $ffa_id
     * @return bool|object
     */
    private function __checkRetailerRecord($ffa_id)
    {
        $country = $this->country['country_name'];
        $sql = "SELECT TOP 1 id
        FROM 
            [$this->schemaName].[$this->reportTable]
        WHERE ffa_id = '$ffa_id' AND [country] ='$country'";

        $results = $this->exec_query($sql);

        if (sqlsrv_num_rows($results) > 0) {
            return sqlsrv_fetch_array($results);
        }

        return false;
    }


    private function getPortalSettingsKey($key)
    {
        $sql = "SELECT TOP 1
            [id],
            [key],
            [value]
        FROM    
            [$this->schemaName].[$this->stagingTable]
        WHERE [key] = '$key'";

        $results = $this->exec_query($sql);

        if (sqlsrv_num_rows($results) > 0) {
            $row = sqlsrv_fetch_array($results);
            return $row;
        }

    }

    private function getOffBusinessHours()
    {
        $workingHoursSQL = "SELECT
           [id],
           [module]
            [key],
            [value]
        FROM
            [$this->schemaName].[$this->stagingTable]
        WHERE [module] = 'working-hours'";

        $workingHours = $this->exec_query($workingHoursSQL);

        if (sqlsrv_num_rows($workingHours) > 0) {

            while ($row = sqlsrv_fetch_array($workingHours)) {
                $value = json_decode($row['value']);
                
                $res['start'] = $value->start;
                $res['end'] = $value->end;
                return $res;
            }
        }
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
}
