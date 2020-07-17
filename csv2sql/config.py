# Global Settings
# cred: /Users/browdavi/.ssh/rds-combined-ca-bundle.pem
# 
aws_region = 'us-west-2'
db_size = 15 #GB
db_machine = 'db.t2.medium'
db_user="test"
db_password = "notarealpassword"
db_instance= "testmetrics"
db_name = "the_db"
db_sg_name = 'testlog'
db_security_group = "sg-c5c8a8a0"
#db_endpoint = "testpdxmysql.cld9sdiplb4g.us-west-2.rds.foobar.com:3306"
#db_endpoint = "testmetrics.cld9sdiplb4g.us-west-2.rds.foobar.com:3306"
ec2_key_name = 'testlog'
ec2_security_group_name = 'testlog'
ec2_instance_type = 't1.micro'
ec2_ami = '?????'


SiteTbl_desc={
  'SiteId_tbl' : 'INT',
  'ClusterName' : 'VARCHAR',
  'ClusterID' : 'VARCHAR',
  'ProjectName' : 'VARCHAR',
  'AWSSiteCode' : 'VARCHAR',
  'VendorName' : 'VARCHAR',
  'VendorSiteName' : 'VARCHAR',
  'VendorSiteAddress' : 'VARCHAR',
  'VendorSiteCity' : 'VARCHAR',
  'VendorSiteState' : 'VARCHAR',
  'VendorSiteCountry' : 'VARCHAR',
  'VendorSiteLatitude' : 'DOUBLE',
  'VendorSiteLongitude' : 'DOUBLE',
  'InProd' : 'INT',
  'InDev' : 'INT',
  'TypeEC2' : 'INT',
  'TypeCritNet' : 'INT',
  'TypeHPC' : 'INT',
  'TypeVPC' : 'INT',
  'TypeProd' : 'INT',
  'TypeCorp' : 'INT',
  'TypeDX' : 'INT',
  'TypeAIV_CF' : 'INT',
  'TypeTCF_CF' : 'INT',
  'TypeGrumpy' : 'INT',
  'Type_X1' : 'INT',
  'Type_X2' : 'INT',
  'Type_X3' : 'INT',
  'Type_X4' : 'INT',
  'TypeOther' : 'INT',
  'LSEvents' : 'INT',
  'SiteUUID' : 'VARCHAR',
  'VendorSitePostalCode' : 'VARCHAR'
}

AssessmentResultsTbl_desc={ 
  'SiteId' : 'INT',
  'idAssessmentTable' : 'INT',
  'VendorSiteVisitDate' :  'DATETIME',
  'testEngineer' : 'VARCHAR',
  'BizDevRepresentative' : 'VARCHAR',
  'Ph1ScoreTotal': 'FLOAT',
  'Ph1ScoreAIA02': 'FLOAT',
  'Ph1ScoreAIA11': 'FLOAT',
  'Ph1ScoreAIA21': 'FLOAT',
  'Ph1ScoreAIA23': 'FLOAT',
  'Ph1ScoreAIA25': 'FLOAT',
  'Ph1ScoreAIA26': 'FLOAT',
  'Ph1ScoreAIA27': 'FLOAT',
  'Ph1ScoreAIA28': 'FLOAT',
  'Ph1ScoreAIA33': 'FLOAT',
  'Ph2ScoreTotal': 'FLOAT',
  'Ph2ScoreAIA02': 'FLOAT',
  'Ph2ScoreAIA11': 'FLOAT',
  'Ph2ScoreAIA21': 'FLOAT',
  'Ph2ScoreAIA23': 'FLOAT',
  'Ph2ScoreAIA25': 'FLOAT',
  'Ph2ScoreAIA26': 'FLOAT',
  'Ph2ScoreAIA27': 'FLOAT',
  'Ph2ScoreAIA28': 'FLOAT',
  'Ph2ScoreAIA33': 'FLOAT',
  'Ph3ScoreTotal': 'FLOAT',
  'Ph3ScoreAIA02': 'FLOAT',
  'Ph3ScoreAIA11': 'FLOAT',
  'Ph3ScoreAIA21': 'FLOAT',
  'Ph3ScoreAIA23': 'FLOAT',
  'Ph3ScoreAIA25': 'FLOAT',
  'Ph3ScoreAIA26': 'FLOAT',
  'Ph3ScoreAIA27': 'FLOAT',
  'Ph3ScoreAIA28': 'FLOAT',
  'Ph3ScoreAIA33': 'FLOAT',
  'CompScoreAIA02': 'FLOAT',
  'CompScoreAIA11': 'FLOAT',
  'CompScoreAIA21': 'FLOAT',
  'CompScoreAIA23': 'FLOAT',
  'CompScoreAIA25': 'FLOAT',
  'CompScoreAIA26': 'FLOAT',
  'CompScoreAIA27': 'FLOAT',
  'CompScoreAIA28': 'FLOAT',
  'CompScoreAIA33': 'FLOAT',
  'CompScoreTotal': 'FLOAT',
  'ReasonsFor' : 'TEXT',
  'ReasonsAgainst' : 'TEXT',
  'ShowStoppers' : 'TEXT',
  'PrelimReportDate' : 'DATETIME',
  'FinalReportDate' : 'DATETIME',
  'SiteDeliverDate' : 'DATETIME',
  'testSelect' : 'INT',
  'BizDevSelect' : 'INT',
  'BizDevSelectDate' : 'DATETIME',
  'AssessmentGUID' : 'VARCHAR',
  'SiteUUID' : 'VARCHAR',
}


AuditResultsTbl_desc = {
  'idAuditTbl' : 'INT',
  'AuditDate' : 'DATETIME',
  'AuditFnlRpt' : 'DATETIME',
  'AuditPrelimRpt' : 'DATETIME',
  'C_SPOFS' : 'INT',
  'C_Risks' : 'INT',
  'SPOFS' : 'INT',
  'Risks' : 'INT',
  'PreviousAI' : 'INT',
  'NewAI' : 'INT',
  'AgreedAI' : 'INT',
  'RemediedAI' : 'INT',
  'SiteId' : 'INT',
  'SiteUUID' : 'VARCHAR',
  'AuditResults' : 'TEXT',
  'AuditActions' : 'TEXT',
}

QuestionDetailTbl_desc = {
'QId' : 'INT' ,
'AssessmentResultsIdx' : 'INT' ,
'QuestionNo' : 'INT' ,
'VendorResponse' : 'VARCHAR' ,
'VendorComment' : 'VARCHAR' ,
'VendorExtComment' : 'VARCHAR' ,
'PrimaryCategory' : 'INT' ,
'SecondaryCategory' : 'INT' ,
'TeritaryCategory' : 'INT' ,
'QuestionnairePhase' : 'INT' ,
'Score' : 'INT' ,
'DCEOverrideAnswer' : 'INT' ,
'DCEOverrideRescore' : 'INT' ,
'DCEOverrideComment' : 'INT' ,
'DCEOverrideFlag' : 'INT' ,
'QuestionnaireGUID' : 'VARCHAR' ,
'AssessmentGUID' : 'VARCHAR' ,
'MaxQuestions' : 'INT' ,
'DocEngVersion' : 'FLOAT',
}

QuestionTbl_desc = {
'QuestionNo' : 'INT' ,
'QuestionText' : 'VARCHAR' ,
'QuestionnaireGUID' : 'VARCHAR' ,
'DocEngVersion' : 'FLOAT',
}

#This dictionary hash is used by the logging module
LOGGING = {
  'version': 1,
  'disable_existing_loggers': True,
  'formatters': {
    'default': {
      'format': '%(levelname)s %(asctime)s %(message)s',
      'datefmt': '%m/%d/%Y %H:%M%S',
    }
  },
  'handlers': {
    'null': {
      'level': 'DEBUG',
      'class': 'logging.NullHandler',
    },
    'console': {
      'level': 'DEBUG',
      'class': 'logging.StreamHandler',
      'formatter': 'default',
    },
    'file': {
      'level': 'INFO',
      'class': 'logging.handlers.TimedRotatingFileHandler',
      'formatter': 'default',
      'filename': 'temp',
      'when': 'M',
      'interval': 1,
    }
  },
  'Loggers': {
    'EndMemo': {
      'level': 'INFO',
      'handlers': ['console','file'],
      'propagate': True,
    }
  }
}

