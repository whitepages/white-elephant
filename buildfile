repositories.remote << "http://www.ibiblio.org/maven2/"

COMMONS  = struct( :logging => 'commons-logging:commons-logging-api:jar:1.1' )
HADOOP   = struct( :core => "org.apache.hadoop:hadoop-core:jar:0.20.2" )
JSON_SIMPLE = struct( :core => "com.googlecode.json-simple:json-simple:jar:1.1" )
PIG      = struct( :core => "org.apache.pig:pig:jar:0.9.0" )

VERSION = '0.1.0'
GROUP = 'com.whitepages'

define "white-elephant" do
  project.version = VERSION
  project.group = GROUP

  compile.with( COMMONS,
                HADOOP,
                JSON_SIMPLE,
                PIG )
  compile.using( :other => [ '-Xlint:all' ] )
  
  package :jar
end
