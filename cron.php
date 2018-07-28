<?php

ini_set('auto_detect_line_endings',TRUE);

include (dirname(__FILE__) . "/config.php");
include (dirname(__FILE__) . "/Infinity.php");

openlog("infinity", LOG_PID | LOG_PERROR, LOG_LOCAL0);

$infinity->currentDir = dirname(__FILE__);
$infinity->uploadFolder = $infinity->currentDir . '/' . UPLOADED_FOLDER;
$infinity->processedFolder = $infinity->currentDir . '/' . PROCESSED_FOLDER;
$infinity->faultyDir = $infinity->currentDir . '/' . FAULTY_FOLDER;

$infinity = new Infinity();

$files = scandir($infinity->uploadFolder);

try {

    $infinity->lockFile = $infinity->currentDir . "/" . "pid.lock";

    if (file_exists($infinity->lockFile)) {
        $content= json_decode(file_get_contents($infinity->lockFile));
        if (!isset($content->pid) || !isset($content->timestamp)) {
            throw new Exception("Some information are missing from the '{$infinity->lockFile}' file", 95);
        }

        $runtime = time() - $content->timestamp;
        switch($runtime) {
            case $runtime > 300:
                $infinity->killProcess(99, "The file '{$content->pid}' has been running for more then 5 minutes. The process will be killed manually");
                break;
            case $runtime > 180:
                throw new Exception("The file '{$content->pid}' has ran for over 3 minutes. Attention is needed", 97);
                break;
            case $runtime > 60:
                throw new Exception("The file '{$content->pid}' is taking more then 1 minute to run", 96);
                break;
        }
        exit;
    } else {
        file_put_contents($infinity->lockFile,
            json_encode(
                array(
                    'pid'     => getmypid(),
                    'timestamp' => time()
                )
            )
        );
    }

    // Initial checks
    if (!is_writable($infinity->currentDir)) {
        $infinity->killProcess(100, "Permission denied on " . dirname(__FILE__));
    }
    if (!is_writable($infinity->uploadFolder)) {
        $infinity->killProcess(101, "Permission denied on " . $infinity->uploadFolder);
    }
    if (!is_writable($infinity->processedFolder)) {
        $infinity->killProcess(102, "Permission denied on " . $infinity->processedFolder);
    }

    // check if "processed" folder already exists. If not, the system will try to create it
    if (!file_exists($infinity->processedFolder)) {
        if (!mkdir($infinity->processedFolder)) {
            $infinity->killProcess(103, "Directory " . PROCESSED_FOLDER . " cannot be created");
        }
    }

    $mysqli = new mysqli(MYSQLI_HOST, MYSQLI_USERNAME, MYSQLI_PASSWORD, MYSQLI_DBNAME);
    if ($mysqli->connect_errno) {
        $infinity->killProcess($mysqli->connect_errno, $mysqli->connect_error);
    }

    // Check if table `events` exists. If not, it will try to create it
    if (!$mysqli->query("SELECT 1 from `events`")) {

        $sql = "
            CREATE TABLE events (
            eventID INT(10) UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            eventDatetime TIMESTAMP NOT NULL,
            eventAction VARCHAR(20) NOT NULL,
            callRef INT(10) NOT NULL,
            eventValue DECIMAL(13,2) NULL,
            eventCurrencyCode VARCHAR(3) NULL
        )";

        if (!$mysqli->query($sql)) {
            $infinity->killProcess($mysqli->errno, $mysqli->error);
        }

    }

    foreach($files as $key => $file) {

        if ($file == "." || $file == "..") continue;
        $csv = $infinity->readCSV($infinity->uploadFolder . '/' . $file);

        foreach($CSV_HEADERS as $hk => $header) {
            if (!array_key_exists($header, $csv['header'])) {
                syslog(LOG_ERR, "Error[104]: The column [{$header}] cannot be found in {$file}");
                $infinity->movefileToFaultyDirectory($file);
                continue 2;
            }
        }

        $sql = "
        INSERT INTO `events`
        (eventID, eventDatetime, eventAction, callRef, eventValue, eventCurrencyCode)
        VALUES
        ";

        foreach($csv['content'] as $kc => $value) {
            if ($kc != 0 && $kc < count($csv['content'])) $sql .= ",";
            $eventDatetime     = $value[$csv['header']['eventDatetime']];
            $eventAction       = $value[$csv['header']['eventAction']];
            $callRef           = $value[$csv['header']['callRef']];
            $eventValue        = floatval($value[$csv['header']['eventValue']]);
            $eventCurrencyCode = $value[$csv['header']['eventCurrencyCode']];

            if ($eventValue == 0) $eventCurrencyCode = '';
            else if (empty($eventCurrencyCode) && $eventValue != 0) {
                syslog(LOG_ERR, "Error[105]: eventCurrencyCode missing (callRef: {$callRef}) on {$file}");
                $infinity->movefileToFaultyDirectory($file);
                continue 2;
            }

            $sql .= "(null, '{$eventDatetime}', '{$eventAction}', '{$callRef}', '{$eventValue}', '{$eventCurrencyCode}')";

        }

        if (!$result = $mysqli->query($sql)) {
            syslog(LOG_ERR, "Error[{$mysqli->errno}]: " . $mysqli->error);
        }

        if (!rename($infinity->uploadFolder . "/" . $file, $infinity->processedFolder . "/" . $file)) {
            syslog(LOG_ERR, "Error[106]: There was a problem moving the file '{$file}' to " . PROCESSED_FOLDER . " directory");
        }
    }

} catch (Exception $e) {
    syslog(LOG_ERR, "Error[{$e->getCode()}]: " . $e->getMessage());
}
