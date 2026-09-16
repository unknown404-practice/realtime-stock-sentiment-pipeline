Write-Host '========================================================' -ForegroundColor Cyan
Write-Host '  Starting Real-Time Stock Sentiment Pipeline Workers' -ForegroundColor Green
Write-Host '========================================================' -ForegroundColor Cyan
Set-Location -Path $PSScriptRoot
& .\.venv\Scripts\python.exe run_pipeline.py
