<?php

class Infinity
{
    public $currentDir;
    public $uploadFolder;
    public $processedFolder;
    public $lockFile;
    public $faultyDir;
    public function __construct() {}

    /**
     * @param (string)$csvFile: The file needing to be parsed and processed
     * @return (array)$csv: Return file content
     */
    public function readCSV($csvFile) {
        $header = false;
        $handle = fopen($csvFile, 'r');

        while (!feof($handle) ) {
            if (!$header) { {
                $csv['header'] = array_flip(fgetcsv($handle, 1024));
                $header = true;
            }
            } else {
                $result = fgetcsv($handle, 1024);
                if (array(null) !== $result && is_array($result)) {
                    $csv['content'][] = $result;
                }
            }

        }
        fclose($handle);
        return $csv;
    }

    /**
     * @param (string)$lockFile: The name of the file which locks further instances
     * @return
     * This function removes the file created by the system when the process is initialised.
     */
    public function removeLockFile($lockFile) {
        try {
            if (unlink($lockFile) === false) {
                throw new Exception("There has been a problem removing '{$lockFile}' file", 98);
            }
        } catch (Exception $e) {
            syslog(LOG_ERR, "Error[{$e->getCode()}]: " . $e->getMessage());
        }
        return;
    }

    /**
     * @param (int)$errorNo: The unique error number
     * @param (string)$ErrorMessage: Description of wht the error is
     * @return
     */
    public function killProcess($errorNo, $ErrorMessage) {
        syslog(LOG_ERR, "Error[{$errorNo}]: " . $ErrorMessage);
        $this->removeLockFile($this->lockFile);
        return;
    }

    /**
     * @param (string)$file: The faulty file
     * This function is created to prevent that faulty uplaoded files get processed every time
     * the PHP script runs. This method move the faulty file to its own dedicated falter to
     * be reviewed later.
     */
    public function movefileToFaultyDirectory($file) {
        if (!file_exists($this->faultyDir)) {
            if (!mkdir($this->faultyDir)) {
                $this->killProcess(107, "Directory '{$this->faultyDir}' cannot be created");
            }
        }
        if (!rename($this->uploadFolder . "/" . $file, $this->faultyDir . "/" . $file)) {
            syslog(LOG_ERR, "Error[108]: There was a problem moving the file '{$file}' to {$this->faultyDir} directory");
        }
        return;
    }

}