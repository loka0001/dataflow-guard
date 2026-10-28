# Attribution & Origin

**Project Name:** DataFlow-Guard  
**Primary Author:** Malek Mesbah (<maleksobhy2004@gmail.com>)  
**License:** MIT License  
**Originality:** Original Work  

## Origin & Context
DataFlow-Guard was created as an enterprise-grade ETL pipeline and data quality enforcement engine inspired by modern data contract standards (Open Data Contract Standard, Great Expectations, and Soda Core).

It directly addresses key data engineering challenges observed across Upwork demand signals:
- Silent data corruption where malformed, negative, or invalid enum records enter production analytical tables.
- Lack of idempotency during repeated pipeline runs causing duplicate or colliding rows.
- Absence of isolated quarantine (Dead Letter Queue) tables for root cause debugging.
- Inability to generate automated executive data quality reports upon batch execution.
