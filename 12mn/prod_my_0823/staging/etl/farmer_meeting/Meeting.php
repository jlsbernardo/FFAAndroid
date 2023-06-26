<?php

require_once(dirname(__FILE__, 3) . '/src/RNA/Database/DB.php');

class Meeting extends DB
{

    protected $ffaTable = 'tbl_ffa_meeting';

    protected $stagingTable = '';

    protected $reportTable = 'farmer_meeting_reports';

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

    /**
     * Check if data is deleted
     *
     * @param  $ffa_id
     * @return void
     */
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
        $checkLastRecord = $this->__checkLastRecordFFASync();
        $lastInserted = ($checkLastRecord) ? $checkLastRecord['last_insert_id'] : null;
        $datePopulate = strtotime('-90 days');
        
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
        AND tmp.category='meeting'
        WHERE
            UNIX_TIMESTAMP(CONVERT_TZ(FROM_UNIXTIME($this->ffaTable.create_on), '".UTC_TIMEZONE."', '".CURRENT_TIMEZONE."'))>=$datePopulate
        GROUP BY 
            $this->ffaTable.id 
        order by
            $this->ffaTable.create_on
        desc";

        $meetingInsertQuery = "";
        $result = $this->exec_query($sql);
        $countMeetingAffectedRows = 0;
        $data = [];
        $country = $this->country['country_name'];
        if ($result->num_rows > 0) {
            while ($row = $result->fetch_assoc()) {
                $deleted = $this->__checkDeleted($row['id']);
                $territory = $row['territory'];

                $zoneRegion = $this->getMeetingZoneRegion($territory);
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

                $meetingRNAFields = array(
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
                    $meetingRNAFields['update_on'] = $row['update_on'];
                }

                if (!empty($row['closed_on']) && !is_null($row['closed_on'])) {
                    $meetingRNAFields['closed_on'] = $row['closed_on'];
                }
                
                if (!empty($row['approved_on']) && !is_null($row['approved_on'])) {
                    $meetingRNAFields['approved_on'] = $row['approved_on'];
                }

                if (!empty($row['execute_on']) && !is_null($row['execute_on'])) {
                    $meetingRNAFields['execute_on'] = $row['execute_on'];
                }

                if (!empty($row['plan_on']) && !is_null($row['plan_on'])) {
                    $meetingRNAFields['plan_on'] = $row['plan_on'];
                }

                if (!empty($row['date']) && !is_null($row['date'])) {
                    $meetingRNAFields['date'] = $row['date'];
                }

                if (!empty($row['marked_on']) && !is_null($row['marked_on'])) {
                    $meetingRNAFields['marked_on'] = $row['marked_on'];
                }

                $data[] = $meetingRNAFields;
                $countMeetingAffectedRows++;
            }

            return [
                'data' => $data,
                'count' => $countMeetingAffectedRows,
                'last_inserted' => $lastInserted
            ];

        } else {
            $message = "No Meeting Records to sync";
            return [
                'data'  => $message,
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
        $count = 0;
        $objVN = new vn_charset_conversion();
        $country = $this->country['country_name'];
        foreach ($data as $meetingRNAField) {
            if (!empty($meetingRNAField['create_on']) && !is_null($meetingRNAField['create_on']) || !empty($meetingRNAField['update_on']) && !is_null($meetingRNAField['update_on'])) {
                // if (date('Y-m-d') == convert_to_datetime($meetingRNAField['create_on'], 'Y-m-d') || date('Y-m-d') == convert_to_datetime($meetingRNAField['update_on'], 'Y-m-d')) {
                    $results = $this->__checkRecordStaging($meetingRNAField);

                    if (sqlsrv_num_rows($results) < 1) {
                        $strColumns = implode(', ', array_keys($meetingRNAField));
                        $strValues =  " '" . implode("', '", array_values($meetingRNAField)) . "' ";
                        $meetingInsertQuery = "INSERT INTO [$this->schemaName].[$this->stagingTable] ({$strColumns}) VALUES ({$strValues});";

                        $result = $this->exec_query($meetingInsertQuery);
                        if ($result) {
                            $count += 1;
                        }
                    } else {
                        $plan_on = (isset($meetingRNAField['plan_on'])) ? 'plan_on = ' . "'{$meetingRNAField['plan_on']}'," : null;
                        $approved_on = (isset($meetingRNAField['approved_on'])) ? 'approved_on =' . "'{$meetingRNAField['approved_on']}'," : null;
                        $closed_on = (isset($meetingRNAField['closed_on'])) ? 'closed_on =' . "'{$meetingRNAField['closed_on']}'," : null;
                        $date = isset($meetingRNAField['date']) ? 'date =' . "'{$meetingRNAField['date']}'," : null;

                        $ffaId = $meetingRNAField['ffa_id'];
                        $participantList = preg_replace('~[\x00\x0A\x0D\x1A\x22\x27\x5C]~u', '\\\$0', $meetingRNAField['participant_list']);
                        $temp_execute =  $meetingRNAField['temp_execute'];

                        $markedBy = !is_null($meetingRNAField['marked_by']) ? $meetingRNAField['marked_by'] : 0;
                        $markedOn = isset($meetingRNAField['marked_on']) ? 'marked_on =' . "'{$meetingRNAField['marked_on']}'," : null;
                        $team = (strtolower($country)=='vietnam') ? $objVN->convert( $meetingRNAField['team']   ) : $meetingRNAField['team'];
                        $team = (strtolower($country)=='thailand') ? $team : trim($team);

                        $meetingUpdateQuery = "
                        UPDATE [$this->schemaName].[$this->stagingTable]
                        SET 
                            update_on= '{$meetingRNAField['update_on']}',
                            update_by = '{$meetingRNAField['update_by']}',
                            {$plan_on}
                            plan_by   = '{$meetingRNAField['plan_by']}',
                            {$approved_on}
                            approved_by = '{$meetingRNAField['approved_by']}',
                            {$closed_on}
                            closed_by      = '{$meetingRNAField['closed_by']}',
                            {$date}
                            team      = '{$team}',
                            deleted = {$meetingRNAField['deleted']},
                            host_name      = '{$meetingRNAField['host_name']}',
                            host_phone      = '{$meetingRNAField['host_phone']}',
                            month      = '{$meetingRNAField['month']}',
                            products      = '{$meetingRNAField['products']}',
                            territory      = '{$meetingRNAField['territory']}',
                            zone      = '{$meetingRNAField['zone']}',
                            region      = '{$meetingRNAField['region']}',
                            participant_list = '{$participantList}',
                            supervisor = '{$meetingRNAField['supervisor']}',
                            marked_by = '{$markedBy}',
                            {$markedOn}
                            temp_execute = '{$temp_execute}',
                            crop_id     = '{$meetingRNAField['crop_id']}',
                            lat      = '{$meetingRNAField['lat']}',
                            lng      = '{$meetingRNAField['lng']}'
                        WHERE ffa_id = '$ffaId' AND report_table = '$this->reportTable';";

                        $result =  $this->exec_query($meetingUpdateQuery);
                        if ($result) {
                            $count += sqlsrv_rows_affected($result);
                        }
                    }
                // }
            }

            
        }

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

    private function getMeetingZoneRegion($territory)
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
            $regionId = $this->getMeetingRegion($zoneId);
            
             return [
                'zone'  => $zoneId,
                'region'    => $regionId
            ];
        }
        

    }

    private function getMeetingRegion($zoneId)
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

        return $supId;
    }

}
