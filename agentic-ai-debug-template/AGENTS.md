# AWS MWAA Environment Context for AI Analysis

**Instructions for AI**: This file provides context about the AWS MWAA environment. Use this information to set your analysis context, then examine all provided files to identify issues, performance problems, and best practices violations. Look beyond just the symptoms listed - analyze logs, DAGs, and configurations comprehensively.

## Environment Details

### MWAA Environment
- **Environment Name**: `united-test-mwaa`
- **MWAA Version**: `2.8.1`
- **Environment Class**: `mw1.xlarge`
- **Region**: `us-east-1`

### Current Configuration
- **Min Workers**: `1`
- **Max Workers**: `10`
- **Scheduler Count**: `4`

## Problem Overview

### When Did Issues Start
`10/03/2025`


### Symptoms Observed
Check all that apply - **AI should look for additional issues beyond these**:
- [ X ] DAG import timeouts
- [ ] Tasks stuck in queued state  
- [ ] Worker crashes/restarts
- [ X ] Scheduler performance issues
- [ X ] Memory/CPU exhaustion
- [ ] Task execution failures
- [ ] Zombie/orphaned tasks
- [ ] Environment updates failing
- [ ] UI performance issues

---

## AI Analysis Instructions

Perform a comprehensive root cause analysis of Apache Airflow MWAA environment stability issues, focusing on code-level anti-patterns that cause worker unavailability, scheduler exhaustion, and database connection pool saturation.

### 1. Environment Context
- Airflow version and MWAA configuration
- Incident history (dates, case numbers, symptoms, downtime)
- Worker configuration (min/max workers, autoscaling settings)
- Scheduler configuration (parsing processes, parse interval)
- Database connection pool settings

### 2. Root Cause Analysis
- Examine logs for error patterns and performance issues
- Analyze DAG code for best practices violations
- Check for resource constraints and bottlenecks
- Identify configuration problems

### 3. Performance Assessment  
- DAG parsing performance
- Task execution efficiency
- Resource utilization patterns
- Scaling issues

### 4. Best Practices Review
- Code quality and structure
- Configuration optimization
- Architecture patterns
- Security considerations

### 5. Specific MWAA Issues
- Environment sizing appropriateness
- Worker scaling patterns
- CloudWatch log analysis
- MWAA-specific limitations

### 6. Report Structure
Generate the following reports:

#### Executive Summary
- Environment overview
- Incident history summary
- Key findings (top 3-5 critical issues)
- Business impact (downtime, pipelines affected)
- Recommended actions (prioritized)

#### Smoking Guns Summary
- Critical blocking operations with code evidence
- Quantitative impact calculations
- Cascade effect diagrams
- Immediate recommendations

#### Detailed Findings
For each anti-pattern found:
- **Category** (Parse-Time, Worker Blocking, Resource Management, etc.)
- **Severity** (Critical, High, Medium, Low)
- **Location** (file path, line numbers)
- **Code Evidence** (actual code snippets)
- **Why It's Bad** (technical explanation)
- **Impact Calculation** (quantitative analysis)
- **Evidence from Incidents** (correlation with incident log)
- **Recommended Fix** (code examples)
- **Expected Improvement** (metrics)

#### Technical Appendices
- Import chain diagrams
- Module caching analysis
- Heavy imports breakdown
- Blocking operations comprehensive analysis
- DAG factory architecture analysis
- Infinite loop tasks analysis
- Blast radius analysis (which DAGs are affected)

**Priority**: Focus on issues that cause the most operational impact first, then address performance optimizations and preventive measures.

### Disclaimer Template

Include this disclaimer in all reports:

```
## DISCLAIMER

This report has been compiled with the assistance of Generative AI. While all 
reasonable efforts have been made to verify findings against actual source code, 
including:

- Direct examination of source files and line numbers
- Cross-referencing findings across multiple code sections
- Validation of configuration settings

This analysis is based on:

- A snapshot of the codebase that may have changed since analysis
- Available code samples which may not represent the complete system
- Assumptions about the production environment based on limited information

**Manual verification is mandatory.** There may be edge cases, context-specific 
considerations, or environmental factors not captured in this analysis.

**Intent of This Report:** To highlight the impact of not following Apache Airflow 
best practices by showing calculated numbers based on code analysis. The actual 
impact in your environment may vary significantly.

This analysis is provided "as-is" without warranties of any kind. Always follow 
your organization's change management and testing procedures.

Always test all changes in non-production environments first and validate the 
impact before deploying to production.
```
---

**Completed By**: `Kiro.dev - NextGen Agentic AI IDE`
**Date**: Add todays date.