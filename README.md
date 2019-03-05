# SCD2_Scala_demo
Case Study
Use Case1: SCD2 Implementation
You are given two look up tables that are present in Hive: PC_POLICY and PCTL_LOSSHISTORYTYPE
The main file pc_policyperiod_orig.csv is in Linux directory /usr/share/usecase/transformation
1) Move the file pc_policyperiod_orig.csv to your HDFS directory(/user/”user_id”/curation) for
eg:/user/dbuser1/transformation/
2) Create External Hive table pc_policyperiod_orig under your database on top of it using the below
schema. Partition this Hive table based on the unix timestamp.

Field:
BasedOnDate STRING,ValidQuote STRING,TotalPremiumRPT INT,TotalPremiumRPT_cur
STRING,MinimumPremium DECIMAL,Locked INT,EditEffectiveDate STRING,ValidReinsurance
INT,SeriesCheckingPatternCode STRING,ArchiveState STRING,ArchiveSchemaInfo
STRING,PNIContactDenorm INT,LocationAutoNumberSeq INT,EditLocked INT,UpdateTime
STRING,RateAsOfDate STRING,JobID INT,ID INT,EstimatedPremium DECIMAL,SingleCheckingPatternCode
STRING,UWCompany STRING,EstimatedPremium_cur STRING,BillingMethod STRING,PeriodID
STRING,TransactionPremiumRPT INT,AssignedRisk STRING,ExcludeReason
STRING,TransactionPremiumRPT_cur STRING,CreateUserID INT,ArchiveFailureID
STRING,AllowGapsBefore STRING,QuoteHidden STRING,BeanVersion INT,FailedOOSEValidation
STRING,Retired INT,BranchName STRING,Preempted STRING,UpdateUserID INT,FuturePeriods
STRING,PrimaryInsuredNameDenorm STRING,CancellationDate STRING,ModelNumber
STRING,TemporaryBranch STRING,PrimaryInsuredName STRING,Segment STRING,TermNumber
STRING,DepositOverridePct STRING,PolicyTermID STRING,WaiveDepositChange STRING,PeriodStart
STRING,ProducerCodeOfRecordID STRING,DoNotPurge INT,PublicID STRING,AltBillingAccountNumber
STRING,TotalCostRPT INT,WrittenDate STRING,TotalCostRPT_cur STRING,CreateTime
STRING,MostRecentModel STRING,PolicyID STRING,ExcludedFromArchive
STRING,AllocationOfRemainder STRING,OverrideBillingAllocation STRING,ArchiveFailureDetailsID
STRING,ModelDate STRING,InvoiceStreamCode STRING,ModelNumberIndex STRING,BaseState
STRING,MostRecentModelIndex STRING,ArchivePartition STRING,CustomBilling
STRING,TransactionCostRPT INT,BranchNumber STRING,FailedOOSEEvaluation STRING,DepositCollected
STRING,TransactionCostRPT_cur STRING,DepositCollected_cur STRING,BasedOnID
STRING,LockingColumn STRING,RefundCalcMethod STRING,ArchiveDate
STRING,BillImmediatelyPercentage STRING,Status STRING,DepositAmount INT,DepositAmount_cur
STRING,PeriodEnd STRING,PreferredCoverageCurrency STRING,PolicyNumber
STRING,PreferredSettlementCurrency STRING,DESCRIPTION STRING,NAME STRING,NAME1 STRING)
