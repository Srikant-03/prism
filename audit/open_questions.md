# Open Questions & Architecture Decisions

Please provide guidance on the following specific product decisions and deployment requirements:

1. **Deployment Target**: Is PRISM intended to remain a local single-user desktop application (distributed via PyInstaller/Electron/Docker Desktop), or is the goal a multi-tenant cloud SaaS deployment (AWS/GCP/Kubernetes)?
2. **Multi-User Authentication**: If deployed as a web application, which authentication mechanism should be adopted (OAuth2/SSO with Google/GitHub, Supabase/Firebase Auth, or enterprise LDAP/SAML)?
3. **Data Residency & Offline LLM Support**: Do your target enterprise users require complete data privacy/air-gapping where no schema or sample data ever leaves the local network (e.g. supporting local Ollama or vLLM models instead of Google Gemini)?
4. **Target Dataset Scale**: What is the maximum dataset size and row count you expect to support (e.g., 500 MB / 5 million rows, or larger datasets requiring Apache Arrow / DuckDB disk files)?
5. **Multi-File ZIP Archive Ingestion Scope**: When a user uploads a ZIP archive with multiple CSV/Excel files, should PRISM automatically join related tables, register them as separate independent tables in DuckDB, or prompt the user with a table selection modal?
6. **Collaboration Feature Scope**: Is real-time collaborative data analysis (multi-cursor shared workspaces via WebSockets) in scope for MVP v1.0, or should the `/api/collab` prototype code and UI tab be temporarily hidden?
7. **Persistence & Session Retention**: How long should user datasets, cleaning recipes, and generated dashboards be retained (ephemeral session-scoped only, or permanently stored in a relational database like PostgreSQL)?
8. **Compliance Frameworks**: Are there specific industry regulatory standards this platform must comply with prior to launch (e.g. HIPAA, GDPR, SOC 2 Type II, ISO 27001)?
