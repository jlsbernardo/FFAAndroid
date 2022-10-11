<?php

if (!function_exists('generated_files')) {

    /**
     * Generated File (.sql & .txt)
     *
     * @param string $directory
     * @param string $scriptName
     * @param string $fileType
     * @param string $fileValue
     * @return
     */
    function generated_files(string $directory, string $scriptName, $fileType, $fileValue)
    {

        if (!file_exists($directory)) {
            mkdir($directory, 0777, true);
        }
        
        file_put_contents($directory. "/{$scriptName}_" . date('Y_m_d') . '.'.$fileType, $fileValue);
        file_put_contents($directory . "/{$scriptName}_" . date('Y_m_d') . '.'. $fileType, $fileValue);
    }
}

if (!function_exists('convert_to_datetime')) {
    /**
     * Convert date time
     *
     * @param string $string
     * @param string $dateTimeFormat
     * @return DateTime
     */
    function convert_to_datetime(string $string, string $dateTimeFormat = 'Y-m-d H:i:s') 
    {
        return date($dateTimeFormat, $string);
    }
}


if (!function_exists('time_in_range')) {
    /**
     * Convert date time
     *
     * @param string $string
     * @param string $dateTimeFormat
     * @return boolean
     */
    function time_in_range(string $start, string $end, $subject)
    {
        if($subject >= $start && $subject <= $end) {
            return true;
        }
        
        return false;
    }
}
