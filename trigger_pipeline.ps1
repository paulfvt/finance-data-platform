$logFile = "C:\Users\paulf\finance-data-platform\trigger_log.txt"
"$(Get-Date) - Script démarré" | Out-File -Append $logFile

try {
    $result = & "C:\Users\paulf\AppData\Local\Programs\DockerDesktop\resources\bin\docker.exe" exec finance_platform_airflow airflow dags trigger extract_bronze_daily 2>&1
    "$(Get-Date) - Résultat : $result" | Out-File -Append $logFile
} catch {
    "$(Get-Date) - ERREUR : $_" | Out-File -Append $logFile
}