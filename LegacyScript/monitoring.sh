#!/bin/bash

# first parameter is date $1


###################################################
#                                                                                                 #
#                                       Streams                                           #
#                                                                                                 #
###################################################
#declare -A STREAM;
#STREAM[0]="Reporter_Loaders";
STREAM[0]="tap3_outbound";
#STREAM[1]="NR_inbound";
#STREAM[2]="NR_outbound";
STREAM[3]="nrtrde_inbound";
STREAM[4]="nrtrde_outbound";
STREAM[5]="tap3_inbound_IR_NR";
STREAM[6]="tap3_outbound_NR";
STREAM[7]="Reporter_Loaders";
STREAM[8]="Reporter_Loaders_ARCH";
STREAM[9]="WLL_MCI_IN";
STREAM[10]="NRTchargeCBS";


declare -r LAST_DAYS=7; # checks tracelogs belong to these days
declare -r MIN_DELAY=600; # seconds
declare -r MIN_BACKLOG=0; # number of files
declare -r REFRESH_TIME=60; # seconds - 60s = 1min
declare -r TITLE="RMS Monitoring"; # title
declare -r VERSION="0.11"; # version
declare -r CRONTAB_TIME=60;
###################################################
#                                                                                                 #
#                                Path Variables                                   #
#                                                                                                 #
###################################################
# stream directory
STDIR="/comptel/elink/install/EventLink/base/status/streams";

# tracelog directory
TRACEDIR="/comptel/elink/install/EventLink/base/tracelog";

# temp directory
TEMP_DIR="/comptel/elink/install/EventLink/base/status/directory-exporter/monitoringlogs";

declare -r RESULT_FILE=${TEMP_DIR}monitor.tmp;
declare -r TOUCH_FILE=${TEMP_DIR}touch.tmp;

declare -r HTML_FILE=${TEMP_DIR}monitor.html;
declare -r WEB_SERVER_PATH="/comptel/elink/install/tomcat-ccp/apache-tomcat-8.0.39/webapps/eventlink/";
declare -r ARCHIVE_PATH="/comptel/elink/install/tomcat-ccp/apache-tomcat-8.0.39/webapps/eventlink/monitor/archive/";


TEMP_MID=${TEMP_DIR}temp_mid.tmp
TEMP_BLT=${TEMP_DIR}temp_blt.tmp
###################################################
#                                                                                                 #
#                                Global Variables                                 #
#                                                                                                 #
###################################################

TMP="";

# command parameters:
ALL=false;
BACKLOGS=false;
AVG=false;
SPECIFIC_DATE=false;
ORDER=0;
JUST_COLLECTOR_DELAY=false;

###################################################
#                                                                                                 #
#                                Const Variables                                  #
#                                                                                                 #
###################################################
# result array
declare -A STRAMES_DETAIL;
STRAMES_DETAIL_LEN=0;


declare -r STREAM_INDEX=1;
declare -r NODE_INDEX=2;
declare -r TYPE_INDEX=3;
declare -r INPUT_INDEX=4;
declare -r LINK_INDEX=5;
declare -r LINK_STATE_INDEX=6;
declare -r BUFFER_INDEX=7;
declare -r EXECUTE_START_INDEX=8;
declare -r EXECUTE_END_INDEX=9;
declare -r LAST_PROCESS_INDEX=10;
declare -r IO_STATE_INDEX=11;
declare -r IO_THROUGHPUT_INDEX=12;
declare -r PERFORMANCE_INDEX=13;
declare -r TIMESLOT_INDEX=14;
declare -r DELAY_INDEX=15;
declare -r SCHEDULE_INDEX=16;
declare -r COLLECTED_TIME_INDEX=17;
declare -r RECORDS_INDEX=18;
declare -r FILES_INDEX=19;
declare -r DISCARD_PATH=20;
declare -r DISCARD_COUNT=21;
declare -r REJECTED_COUNT=22;
declare -r STOPPED_INDEX=23;
declare -r FAILED_INDEX=24;
declare -r FAILED_TIME_INDEX=25;
#declare -r AUDIT_FILE_PATH_INDEX=26;
declare -r REJECTED_TODAY_COUNT=26;
declare -r DISCARD_TODAY_COUNT=27;

#declare -A NODE_BUFFER;
#declare -A COLLECTOR_NODE;

#collector_node=();

# for daysaving time changes. it occurs two times in a year.
declare TZ_CHANGE_ON="20170322";
declare TZ_CHANGE_PREV="20170321";







###################################################
#                                                                                                 #
#                                        Functions                                        #
#                                                                                                 #
###################################################

write_log() {
        printf "`date +"%Y%m%d %H:%M:%S"` ${1}\n";
}

getsubdir() {
   TMP=();
   TMP=$(find "${1}" -maxdepth 1 -mindepth 1  -type d | sort);
}


getfileswithname() {
   TMP=();
   TMP=$(find ${1} -iname "${2}"|sort);
}


get_last_part_of_path() {
        IFS='/' read -ra SPLIT <<< "${1}";
        len=${#SPLIT[@]};
        TMP=$(echo $node | cut -d'/' -f $len);
}


convert_epoch_to_time() {
        TMP=`date +"%H:%M:%S" -d @${1}`;
}

convert_epoch_to_date() {
        TMP=`date +"%Y%m%d" -d @${1}`;
}

convert_epoch_to_date_time() {
        TMP=`date +"%Y%m%d %H:%M:%S" -d @${1}`;
}

get_now_epoch() {
        TMP=`date +"%s"`;
}

get_yesterday() {
        TMP=`date --date="yesterday" +"%Y%m%d"`;
}

# D: Day, H: Hour, M: Minute
convert_seconds_to_DHM() {
        delay=${1};
        d=$(( $delay / 86400 ));
        delay=$(( $delay % 86400 ));
        h=$(( $delay / 3600 ));
        delay=$(( $delay % 3600 ));
        m=$(( $delay / 60 ));
        #delay="${d}d ${h}h"; #"${h}:${m}:${s}";#$(date +"%H:%M:%S" -d @${delay});
        TMP="${d}d${h}h${m}m";
}


get_streams_detail() {
        # process each stream
        i=0;
        STRAMES_DETAIL=();
        today=$1;

        get_yesterday;
        yesterday="${TMP}210000";

        touch -d $today $TOUCH_FILE;

        echo "`date +"%Y%m%d %H:%M:%S"` stream(s) details collecting ... ";
        #printf "? `date +"%Y%m%d %H:%M:%S"` ${RCol}stream(s) details collecting ... "; #                                                                   \t\t       ${BBlu}?";
        for st in "${STREAM[@]}"; do
                #echo $st;
                #******* stream buffer ********
                if [[ "${st}" != "" ]]; then
                        printf "\t%-107s\n" "$st";

                        node_type="";
                        # process each node
                        while read node; do
                                # node name

                                #echo $node;
                                get_last_part_of_path "$node";
                                node_name=$TMP;
                                #echo $node_name;

                                if [[ "${node_name}" == "mail"* || "${node_name}" == "rap_import"* ]]; then
                                        continue;
                                fi


                                node_type="";
                                schedule=false;

                                # process control directory of the node
                                #flag2=false;
                                if [ -f "${node}/control/1/config" ]; then

                                        database_loader=0;
                                        database_table="";
                                        elr_home="";


                                        # read input buffer path from config file
                                        while read LINE; do
                                                #echo $LINE;
                                                flag=false;

                                                # get node type
                                                if [[ $LINE == NodeType*  ]]; then
                                                        node_type=$(echo $LINE | cut -d'"' -f 2);
                                                fi

                                                # is a database loader node
                                                if [[ $LINE == ElrHome* ]]; then
                                                        database_loader=1;
                                                        elr_home=$(echo $LINE | cut -d'"' -f 2);
                                                fi

                                                # is a database loader node
                                                if [[ $LINE == DatabaseTable* ]]; then
                                                        database_table=$(echo $LINE | cut -d'"' -f 2);
                                                fi



                                                input_path=""; link_state=""; #link_name="";
                                                if [[ $node_type == *"collector"* ]]; then
                                                        if [[ $LINE == SourceDirectory* ]]; then
                                                                input_path=$(echo $LINE | cut -d'"' -f 2);
                                                                flag=true;
                                                        elif [[ $LINE == *DataStorage* ]]; then
                                                                flag=true;
                                                                input_path="DATA_STORAGE";
                                                        fi
                                                elif [[ $LINE == InDataPath* ]]; then
                                                        input_path=$(echo $LINE | cut -d':' -f 2 | cut -d'"' -f 1);
                                                        flag=true;
                                                elif [[ $database_loader -eq 1 ]]; then
                                                        database_loader=0;
                                                        input_path="${elr_home}/upload/${database_table}/in/";
                                                        flag=true;
                                                fi

                                                # echo ${input_path};

                                                if [[ -n ${input_path} ]]; then
                                                        # if directory not exist
                                                        if [[ ! -d "$input_path" ]]; then
                                                                link_state='NOT_EXIST';
                                                        else
                                                                link_state='OK';
                                                        fi
                                                fi


                                                if [[ $flag == true ]]; then
                                                        #echo "flag: " ${node};

                                                        (( i++ ));
                                                        # stream name
                                                        flag2=true;
                                                        STRAMES_DETAIL[$i, $STREAM_INDEX]="$st";
                                                        STRAMES_DETAIL[$i, $NODE_INDEX]="$node_name";
                                                        STRAMES_DETAIL[$i, $TYPE_INDEX]="$node_type";
                                                        STRAMES_DETAIL[$i, $INPUT_INDEX]="$input_path";
                                                        #STRAMES_DETAIL[$i, $LINK_INDEX]="$link_name";
                                                        STRAMES_DETAIL[$i, $LINK_STATE_INDEX]="$link_state";
                                                        STRAMES_DETAIL[$i, $SCHEDULE_INDEX]="$schedule";
                                                        #STRAMES_DETAIL[$i, $DISCARD_PATH]="$discard_path";
                                                        STRAMES_DETAIL[$i, $DISCARD_COUNT]=0;
                                                        STRAMES_DETAIL[$i, $DISCARD_TODAY_COUNT]=0;
                                                        STRAMES_DETAIL[$i, $REJECTED_COUNT]=0;
                                                        STRAMES_DETAIL[$i, $REJECTED_TODAY_COUNT]=0;
                                                        # echo $discard_path;

                                                        STRAMES_DETAIL[$i, $STOPPED_INDEX]=0;
                                                        if [ ! -f "${node}/control/1/heartbeat" ]; then
                                                                #if [[ "${STRAMES_DETAIL[$i, $NODE_INDEX]}" != "mail"* && "${STRAMES_DETAIL[$i, $NODE_INDEX]}" != "rap_import"* ]]; then
                                                                STRAMES_DETAIL[$i, $STOPPED_INDEX]=1;
                                                                #echo "heartbeat" ${node};
                                                                #fi
                                                        fi
                                                fi
                                        done <"${node}/control/1/config";       #${ctl}/config;



                                        #break;
                                #done # end for control
                                fi # end for control



                                if [[ $flag2 == true ]]; then
                                        #echo "flag2: " ${node};


                                        flag2=false;
                                        # discarded file count
                                        STRAMES_DETAIL[$i, $DISCARD_COUNT]=$(ls "${node}/discarded/" | wc -l);
                                        STRAMES_DETAIL[$i, $DISCARD_TODAY_COUNT]=$(find "${node}/discarded/" -type f -newer $TOUCH_FILE | wc -l);

                                        # rejected records count *************************************
                                        # total rejected cdrs
                                        STRAMES_DETAIL[$i, $REJECTED_COUNT]=`find "${node}/rejected/" -name "*[0-9]" -type f | cut -d"}" -f 2 | cut -d"_" -f 2 | awk '{c += $1} END { c=(c=="")?0:c; print c }'`;

                                        count=`find ${node}/rejected/ -name "2*" -type d -printf "%f\n" | awk '$1>=$yesterday {print $1}' | wc -l`;
                                        #echo $count;
                                        if [[ $count -gt 0 ]]; then
                                                sum=$(find `find ${node}/rejected/ -name "2*" -type d | awk -F/ '$NF+0>='$yesterday''` -regex ".*[0-9]$" -type f | awk -F_ '{x=NF-2;c+=$x}; END {c=(c=="")?0:c; print c}');

                                                #sum=0;
                                                #for dir in `find ${node}/rejected/*/ -name "2*" -maxdepth 1 -type d | awk -F/ '$1>=$yesterday {print $1}'`; do
                                                #       s=`find ${dir} -name "*[0-9]" -type f | cut -d"}" -f 2 | cut -d"_" -f 2 | awk '{c += $1} END { print c }'`;
                                                        #s=2;
                                                #       sum=$((sum+s));
                                                #       echo $s;
                                                #done
                                                #echo $sum;
                                                STRAMES_DETAIL[$i, $REJECTED_TODAY_COUNT]=$sum;

                                                #STRAMES_DETAIL[$i, $REJECTED_TODAY_COUNT]=`find ${node}/rejected/*/${today}* -regex ".*[0-9]$" -type f | awk -F/ 'substr($NF,1,10)+0 >= $epoch {match($NF,".*_(.*)_.*_.*$",a); c+=a[1]}; END {print c}'`;
                                        fi

                                        # audit file
                                        if [ -f "${node}/storage/1/audit_info" ]; then
                                                audit_info=`cat "${node}/storage/1/audit_info"`;
                                                audit_info="${audit_info//  / }";
                                                STRAMES_DETAIL[$i, $LAST_PROCESS_INDEX]=`echo ${audit_info} | cut -d" " -f 1`;

                                                epoch=`echo ${audit_info} | cut -d" " -f 2`;
                                                convert_epoch_to_date_time $epoch;
                                                STRAMES_DETAIL[$i, $COLLECTED_TIME_INDEX]=$TMP;
                                        fi
                                else
                                        #echo "heartbeat2" ${node};
                                        if [ ! -f "${node}/control/1/heartbeat" ]; then
                                                (( i++ ));
                                                # stream name
                                                STRAMES_DETAIL[$i, $STREAM_INDEX]="$st";
                                                STRAMES_DETAIL[$i, $NODE_INDEX]="$node_name";
                                                STRAMES_DETAIL[$i, $TYPE_INDEX]="$node_type";
                                                STRAMES_DETAIL[$i, $DISCARD_COUNT]=0;
                                                STRAMES_DETAIL[$i, $REJECTED_COUNT]=0;
                                                STRAMES_DETAIL[$i, $STOPPED_INDEX]=1;
                                        fi
                                fi


                        done < <(find "${STDIR}/${st}/nodes/" -maxdepth 1 -mindepth 1  -type d | sort);
                fi
        done
        echo "done. `date +"%Y%m%d %H:%M:%S"` ${i}";

        STRAMES_DETAIL_LEN=${i};
}




count_backlogs() {

        write_log "node(s) backlogs checking ... ";
        #printf "? `date +"%Y%m%d %H:%M:%S"` ${RCol}node(s) backlogs checking ... ";#                                                                  \t               ${BBlu}?";
        for (( i=1;i<=${STRAMES_DETAIL_LEN};i++ )); do
                count=0;
                if [[ ${STRAMES_DETAIL[$i, $INPUT_INDEX]} != "" && ${STRAMES_DETAIL[$i, $LINK_STATE_INDEX]} == "OK" ]]; then
                        # count number of files in $INPUT_INDEX
                        count=$(find ${STRAMES_DETAIL[$i, $INPUT_INDEX]} -maxdepth 1 -mindepth 1 -type f | wc -l);
                fi
                STRAMES_DETAIL[$i, $BUFFER_INDEX]="${count}";
        done
        echo "done. `date +"%Y%m%d %H:%M:%S"`";
}


count_discards() {
        write_log "node(s) discards checking ... ";
        #printf "? `date +"%Y%m%d %H:%M:%S"` ${RCol}node(s) discards checking ... ";#                                                                  \t               ${BBlu}?";
        for (( i=1;i<=${STRAMES_DETAIL_LEN};i++ )); do
                count=0;
                if [ ${STRAMES_DETAIL[$i, $DISCARD_PATH]} != "" ]; then
                        # count number of files in $INPUT_INDEX
                        count=$(find ${STRAMES_DETAIL[$i, $DISCARD_PATH]} -maxdepth 1 -mindepth 1 -type f | wc -l);
                fi
                STRAMES_DETAIL[$i, $DISCARD_COUNT]="${count}";
        done
        echo "done. `date +"%Y%m%d %H:%M:%S"`";
}


process_trace_files() {
        trace_date=${1};
        avg=${2};
        all=${3};
        specific_date=${4};

        tracefiles=();


        #echo $all;
        #echo $avg;
        #echo $specific_date;

        echo "`date +"%Y%m%d %H:%M:%S"` tracelogs checking ... ";
        #printf "? `date +"%Y%m%d %H:%M:%S"` ${RCol}tracelogs checking ... ${BBlu}?";
        #printf "\n?${BYel} \tTraceLog Date: ${trace_date} ${RCol}                 ${BBlu}?";
        #printf "\n?${BYel} \tStreams:                                                                     ${BBlu}?";

        pre_st=""; pre_node="";
        tracefiles_pattern="";
        for (( i=1;i<=${STRAMES_DETAIL_LEN};i++ )); do

                if [[ ${pre_st} != ${STRAMES_DETAIL[$i, $STREAM_INDEX]} ]]; then
                        pre_st=${STRAMES_DETAIL[$i, $STREAM_INDEX]};
                        printf "\t%-107s\n" "${STRAMES_DETAIL[$i, $STREAM_INDEX]}";
                fi


                if [[ $pre_node != ${STRAMES_DETAIL[$i, $NODE_INDEX]} ]]; then
                        pre_node=${STRAMES_DETAIL[$i, $NODE_INDEX]};

                        # number of trace files
                        number=${#tracefiles[@]};

                        # process each file for execute time
                        start_time=""; end_time="";

                        #if [[ ${JUST_COLLECTOR_DELAY} == false ]]; then
                                #if [[ ${specific_date} == false && ${all} == true ]]; then
                                        if [[ ${STRAMES_DETAIL[$i, $SCHEDULE_INDEX]} == true ]]; then
                                                GREPPED="`find ${TRACEDIR} -iname "execution_trace_${STRAMES_DETAIL[$i, $STREAM_INDEX]}_2*" -exec grep "\[${STRAMES_DETAIL[$i, $NODE_INDEX]}\]  Scheduled execution.*finished" {} \; | sort | tail -1`";
                                                if [[ ${GREPPED} != "" ]]; then # finished

                                                        end_time=`echo "${GREPPED}" | cut -d ' ' -f 2 | cut -d '.' -f 1`;
                                                        addToTime_doubleDot $end_time;
                                                        end_time=${TMP};
                                                        end_time="`echo "${GREPPED[1]}" | cut -d ' ' -f 1` ${end_time}";
                                                        #break;
                                                #elif [[ ${#GREPPED[@]} == 1 ]]; then # running
                                                        #start_time=`echo "${GREPPED[0]}" | cut -d ' ' -f 2 | cut -d '.' -f 1`;

                                                        #addToTime_doubleDot $start_time;
                                                        #start_time=${TMP};
                                                        #start_time="`echo "${GREPPED[0]}" | cut -d ' ' -f 1` ${start_time}";
                                                        #break;
                                                fi
                                        #else
                                                # start will be process in last collected or processed file section
                                                # end will be process in I/O performance section
                                        fi


                                        if [[ $start_time == "" ]]; then
                                                start_time="-";
                                        fi

                                        if [[ $end_time == "" ]]; then
                                                end_time="-";
                                        fi
                                        STRAMES_DETAIL[$i, $EXECUTE_START_INDEX]=$start_time;
                                        STRAMES_DETAIL[$i, $EXECUTE_END_INDEX]=$end_time;
                                #fi
                        #fi
                        ############################################################
                        #if [[ ${specific_date} == false ]]; then
                                # echo 5555;
                                # process each file for last collected or processed file
                                file_date=`date +"%Y%m%d"`;
                                #last_process_file=""; collected_time=""; collected_date=""
                                #if [[ ${STRAMES_DETAIL[$i, $TYPE_INDEX]} == "collector" ]]; then
                                #       if [[ "${STRAMES_DETAIL[$i, $NODE_INDEX]}" != "hur_report" && "${STRAMES_DETAIL[$i, $NODE_INDEX]}" != "mail_parser" ]]; then

                                                #printf "${STRAMES_DETAIL[$i, $NODE_INDEX]}   ";

                                                #echo "end";

                                                # last 30 trace logs files
                                                #for (( f=0;f<${LAST_DAYS};f++ )); do
                                                        #GREPPED="`find ${TRACEDIR} -iname "execution_trace_${STRAMES_DETAIL[$i, $STREAM_INDEX]}_${file_date}*" -exec grep "\[${STRAMES_DETAIL[$i, $NODE_INDEX]}\].*: Collected file.*" {} \; | sort -r | head -1`";
                                                        #if [[ ${GREPPED} != "" ]]; then # running
                                                                #echo ${GREPPED};
                                                                #last_process_file=`echo "${GREPPED}" | cut -d ' ' -f 1,2,12`;
                                                                #collected_date=`echo "${GREPPED}" | cut -d ' ' -f 1`;
                                                                #collected_time=`echo "${GREPPED}" | cut -d ' ' -f 2 | cut -d '.' -f 1`;
                                                                #addToTime_doubleDot $collected_time;
                                                                #collected_time=${TMP};
                                                                #STRAMES_DETAIL[$i, $COLLECTED_TIME_INDEX]="${collected_date} ${collected_time}";
                                                                #echo "${STRAMES_DETAIL[$i, $COLLECTED_TIME_INDEX]}";
                                                                #break;
                                                        #else
                                                                #if [[ $TZ_CHANGE_ON -eq $file_date ]]; then
                                                                #       file_date=$TZ_CHANGE_PREV;
                                                                #fi
                                                                #file_date=`date +"%Y%m%d" -d "${file_date} 1 day ago"`;
                                                        #fi
                                                #done
                                        #fi
                                #el
                                if [[ ${STRAMES_DETAIL[$i, $TYPE_INDEX]} == "distributor"* && ${JUST_COLLECTOR_DELAY} == false ]]; then
                                        #file_date=`date +"%Y%m%d"`;

                                        # last $LAST_DAYS trace logs files
                                        for (( f=0;f<${LAST_DAYS};f++ )); do
                                                GREPPED="`find ${TRACEDIR} -iname "execution_trace_${STRAMES_DETAIL[$i, $STREAM_INDEX]}_${file_date}*" -exec grep "\[${STRAMES_DETAIL[$i, $NODE_INDEX]}\].*: Distributed file .*" {} \; | sort -r | head -1`";
                                                if [[ ${GREPPED} != "" ]]; then # running
                                                        #echo ${GREPPED};
                                                        last_process_file=`echo "${GREPPED}" | cut -d ' ' -f 1,2,12`;
                                                        #echo ${last_process_file};
                                                        collected_date=`echo "${GREPPED}" | cut -d ' ' -f 1`;
                                                        collected_time=`echo "${GREPPED}" | cut -d ' ' -f 2 | cut -d '.' -f 1`;
                                                        addToTime_doubleDot $collected_time;
                                                        collected_time=${TMP};
                                                        STRAMES_DETAIL[$i, $COLLECTED_TIME_INDEX]="${collected_date} ${collected_time}";
                                                        break;
                                                else
                                                        if [[ $TZ_CHANGE_ON -eq $file_date ]]; then
                                                                file_date=$TZ_CHANGE_PREV;
                                                        fi
                                                        file_date=`date +"%Y%m%d" -d "${file_date} 1 day ago"`;
                                                fi
                                        done


                                        if [[ $last_process_file == "" ]]; then
                                                last_process_file="-";
                                        else
                                                #if [[ ${STRAMES_DETAIL[$i, $SCHEDULE_INDEX]} == false ]]; then

                                                        STRAMES_DETAIL[$i, $EXECUTE_START_INDEX]=`echo "${last_process_file}" | cut -d ' ' -f 2 | cut -d '.' -f 1`;
                                                        addToTime_doubleDot ${STRAMES_DETAIL[$i, $EXECUTE_START_INDEX]};
                                                        STRAMES_DETAIL[$i, $EXECUTE_START_INDEX]=${TMP};
                                                        STRAMES_DETAIL[$i, $EXECUTE_START_INDEX]="`echo "${last_process_file}" | cut -d ' ' -f 1` ${STRAMES_DETAIL[$i, $EXECUTE_START_INDEX]}";
                                                        last_process_file=`echo ${last_process_file} | cut -d ' ' -f 3`;
                                                #fi
                                        fi

                                        STRAMES_DETAIL[$i, $LAST_PROCESS_INDEX]=$last_process_file;
                                fi


                        #fi




                        ############################################################
                        ############################################################
                        ############################################################
                        # process blt nodes for records statistics
                        # businesslogic
                        if [[ ${JUST_COLLECTOR_DELAY} == false ]]; then
                                if [[ "${STRAMES_DETAIL[$i, $TYPE_INDEX]}" == *"blt"* || ${STRAMES_DETAIL[$i, $TYPE_INDEX]} == "decoder"* || ${STRAMES_DETAIL[$i, $NODE_INDEX]} == *"agr"* ]]; then
                                        #index=0;
                                        if [[ ${STRAMES_DETAIL[$i, $NODE_INDEX]} == *"agr"* ]]; then

                                                index=3;
                                        else
                                                index=4;
                                        fi

                                        if [[ `ls ${TRACEDIR}/execution_trace_${STRAMES_DETAIL[$i, $STREAM_INDEX]}_${trace_date}_* | wc -l` == 1 ]]; then

                                                (( index-- ));
                                        fi

                                        records=`grep "\[${STRAMES_DETAIL[$i, $NODE_INDEX]}\].* Wrote .* records" ${TRACEDIR}/execution_trace_${STRAMES_DETAIL[$i, $STREAM_INDEX]}_${trace_date}_* | cut -d ':' -f ${index} | cut -d ' ' -f 3 | awk '{sum += $1} END { print sum }'`;
                                        files=`grep "\[${STRAMES_DETAIL[$i, $NODE_INDEX]}\].* Wrote .* records" ${TRACEDIR}/execution_trace_${STRAMES_DETAIL[$i, $STREAM_INDEX]}_${trace_date}_* | wc -l`;
                                        if [[ $records == "" ]]; then
                                                records=0;
                                        fi
                                        STRAMES_DETAIL[$i, $RECORDS_INDEX]=$records;
                                        STRAMES_DETAIL[$i, $FILES_INDEX]=$files;
                                fi
                        fi



                        ############################################################
                        ############################################################
                        ############################################################
                        # disabled and aborted nodes                    aborted and was disabled
                        STRAMES_DETAIL[$i, $FAILED_INDEX]=0;
                        #echo 2;
                        #grep -nE "\[${STRAMES_DETAIL[$i, $NODE_INDEX]}\] .* (aborted and was disabled|Node index .* failed)" ${TRACEDIR}/execution_trace_${STRAMES_DETAIL[$i, $STREAM_INDEX]}_${trace_date}_* | tail -1;
                        error_line=`grep -nE "\[${STRAMES_DETAIL[$i, $NODE_INDEX]}\] .* (aborted and was disabled|Node index .* failed)" ${TRACEDIR}/execution_trace_${STRAMES_DETAIL[$i, $STREAM_INDEX]}_${trace_date}_* | tail -1`;
                        #error_line="";
                        if [[ "${error_line}" != "" ]]; then
                                #echo $error_line;
                                #start_line=`grep -n "\-\-\-\-\[${STRAMES_DETAIL[$i, $NODE_INDEX]}\]\-\-\-\-" ${TRACEDIR}/execution_trace_${STRAMES_DETAIL[$i, $STREAM_INDEX]}_${trace_date}_* | tail -1 | cut -d":" -f 1,2`;
                                #if [[ "${start_line}" != "" ]]; then
                                        if [ ${STRAMES_DETAIL[$i, $STOPPED_INDEX]} -eq 1 ]; then
                                                STRAMES_DETAIL[$i, $FAILED_INDEX]=1;

                                                date_time=`echo $error_line | cut -d":" -f 3 | cut -d " " -f 1,2 | cut -d "." -f 1`;
                                                STRAMES_DETAIL[$i, $FAILED_TIME_INDEX]=$date_time;
                                        fi
                        fi

                fi


                # calculate delay
                if [[ ${STRAMES_DETAIL[$i, $TYPE_INDEX]} == "collector" || ${STRAMES_DETAIL[$i, $TYPE_INDEX]} == "distributor"* ]]; then
                        end=${STRAMES_DETAIL[$i, $COLLECTED_TIME_INDEX]};
                        #echo ${STRAMES_DETAIL[$i, $TYPE_INDEX]};
                        delay=0;
                        if [[ ${end} != "-" && ! -z ${end} ]]; then
                                #echo " ${end}";
                                end=$(date +"%s" -d "${end}");
                                date_now=$(date +"%s");
                                delay=$(( date_now - end ));
                                #echo $delay;
                                if [[ ! $delay -gt $MIN_DELAY ]]; then
                                        delay=0;
                                fi;
                        fi

                        STRAMES_DETAIL[$i, $DELAY_INDEX]=$delay;
                fi
        done


        echo "done. `date +"%Y%m%d %H:%M:%S"`";

        #write_stream_detail;
}


addToTime_doubleDot() {
        time=${1};
        #echo "time ${time}"
        t=""; ix=0;
        TMP="";
        for c in `echo "${time}" | sed -e 's/\(.\)/\1\n/g'`; do
                if [ $ix == 2 ] || [ $ix == 4 ]; then
                        t="${t}:";
                fi
                t="${t}${c}";
                (( ix++ ));
        done
        TMP=$t;
}


write_stream_detail() {

        if [ -f "${RESULT_FILE}" ]; then
                rm -f ${RESULT_FILE};
        fi

        #for (( j=1;j<=$STRAMES_DETAIL_LEN;j++ )); do
        #       echo -e "stream*${STRAMES_DETAIL[$j, $STREAM_INDEX]}\tnode*${STRAMES_DETAIL[$j, $NODE_INDEX]}\tbacklogs*${STRAMES_DETAIL[$j, $BUFFER_INDEX]}\tlast_end*${STRAMES_DETAIL[$j, $EXECUTE_END_INDEX]}\tlast_file*${STRAMES_DETAIL[$j, $LAST_PROCESS_INDEX]}\tI/O_state*${STRAMES_DETAIL[$j, $IO_STATE_INDEX]}\tthroughput*${STRAMES_DETAIL[$j, $IO_THROUGHPUT_INDEX]}\tperformance*${STRAMES_DETAIL[$j, $PERFORMANCE_INDEX]}\ttime_slot*${STRAMES_DETAIL[$j, $TIMESLOT_INDEX]}\tdelay*${STRAMES_DETAIL[$j, $DELAY_INDEX]}\tcollected*${STRAMES_DETAIL[$j, $COLLECTED_TIME_INDEX]}\trecords*${STRAMES_DETAIL[$j, $RECORDS_INDEX]}\tfiles*${STRAMES_DETAIL[$j, $FILES_INDEX]}\tdiscard_count*${STRAMES_DETAIL[$j, $DISCARD_COUNT]}" >>${RESULT_FILE};
        #done
        write_log "writing streams details ... ";
        for (( j=1;j<=$STRAMES_DETAIL_LEN;j++ )); do
                echo -e "stream*${STRAMES_DETAIL[$j, $STREAM_INDEX]}\tnode*${STRAMES_DETAIL[$j, $NODE_INDEX]}\tnode_type*${STRAMES_DETAIL[$j, $TYPE_INDEX]}\tinput_path*${STRAMES_DETAIL[$j, $INPUT_INDEX]}\tlink_name*${STRAMES_DETAIL[$j, $LINK_INDEX]}\tlink_status*${STRAMES_DETAIL[$j, $LINK_STATE_INDEX]}\tbacklogs*${STRAMES_DETAIL[$j, $BUFFER_INDEX]}\tlast_start*${STRAMES_DETAIL[$j, $EXECUTE_START_INDEX]}\tlast_end*${STRAMES_DETAIL[$j, $EXECUTE_END_INDEX]}\tlast_file*${STRAMES_DETAIL[$j, $LAST_PROCESS_INDEX]}\tI/O_state*${STRAMES_DETAIL[$j, $IO_STATE_INDEX]}\tthroughput*${STRAMES_DETAIL[$j, $IO_THROUGHPUT_INDEX]}\tperformance*${STRAMES_DETAIL[$j, $PERFORMANCE_INDEX]}\ttime_slot*${STRAMES_DETAIL[$j, $TIMESLOT_INDEX]}\tdelay*${STRAMES_DETAIL[$j, $DELAY_INDEX]}\tschedule*${STRAMES_DETAIL[$j, $SCHEDULE_INDEX]}\tcollected*${STRAMES_DETAIL[$j, $COLLECTED_TIME_INDEX]}\trecords*${STRAMES_DETAIL[$j, $RECORDS_INDEX]}\tfiles*${STRAMES_DETAIL[$j, $FILES_INDEX]}\tdiscard_path*${STRAMES_DETAIL[$j, $DISCARD_PATH]}\tdiscard_count*${STRAMES_DETAIL[$j, $DISCARD_COUNT]}\treject_count*${STRAMES_DETAIL[$j, $REJECTED_COUNT]}\tstopped*${STRAMES_DETAIL[$j, $STOPPED_INDEX]}\tfailed*${STRAMES_DETAIL[$j, $FAILED_INDEX]}\tfailed_time*${STRAMES_DETAIL[$j, $FAILED_TIME_INDEX]}\ttoday_reject*${STRAMES_DETAIL[$j, $REJECTED_TODAY_COUNT]}\ttoday_discard*${STRAMES_DETAIL[$j, $DISCARD_TODAY_COUNT]}" >>${RESULT_FILE};
                #echo "*******************" >>${RESULT_FILE};
        done
        echo "done. `date +"%Y%m%d %H:%M:%S"`";
}


write_buffers_html() {
        # Backlogs ******************************************************************************
        grep "" ${RESULT_FILE} | cut -d$'\t' -f $STREAM_INDEX,$NODE_INDEX,$BUFFER_INDEX > ${TEMP_MID};

        if [ -f "${TEMP_MID}s" ]; then
                rm -f "${TEMP_MID}s";
        fi
        echo "<div class=\"normal_div\">
        <table class=\"table table-striped table-bordered table-hover\" id=\"backlogs_table\">
        <caption>Buffer(s)</caption>
        <thead><tr class=\"table_header\"><th>Stream</th><th>Node</th><th>#</th></tr></thead>
        <tbody>" >>${HTML_FILE};
        while read line; do
                IFS=$'\t' read -a array <<< "`echo "${line}" | cut -d$'\t' -f 1,2,3`";
                if [[ ${array[2]} != 'backlogs*0' ]]; then
                        printf "%s %s %'d\n" "`echo ${array[0]} | cut -d "*" -f 2`" "`echo ${array[1]} | cut -d "*" -f 2`" "`echo ${array[2]} | cut -d "*" -f 2`" >> "${TEMP_MID}s";
                fi
        done < "${TEMP_MID}";

        if [ -f "${TEMP_MID}s" ]; then
                #cat "${TEMP_MID}s";
                sort -rn -k 3 "${TEMP_MID}s" > ${TEMP_MID};
                #cat ${TEMP_MID};

                while read line; do
                        count=`echo $line | cut -d" " -f 3`;
                        count_int=${count//,/};
                        if [ $count_int -ge $MIN_BACKLOG ]; then
                                echo "<tr><td>"`echo $line | cut -d" " -f 1`"</td>" >> ${HTML_FILE};
                                echo "<td>"`echo $line | cut -d" " -f 2`"</td>" >> ${HTML_FILE};
                                echo "<td>"`echo $count`"</td></tr>" >> ${HTML_FILE};
                        fi
                done < "${TEMP_MID}";
        fi


        echo "</tbody></table></div>" >>${HTML_FILE};
}

write_discards_html() {
        # Discard ******************************************************************************
        grep "" ${RESULT_FILE} | cut -d$'\t' -f $STREAM_INDEX,$NODE_INDEX,$DISCARD_COUNT,$DISCARD_TODAY_COUNT > ${TEMP_MID};

        #cat ${TEMP_MID};

        if [ -f "${TEMP_MID}s" ]; then
                #echo "removing mids";
                rm -f "${TEMP_MID}s";
        fi

        #cat ${TEMP_MID};

        echo "<div class=\"normal_div\">
        <table class=\"table table-striped table-bordered table-hover\" id=\"discard_table\">
        <caption>Discard(s)</caption>
        <thead><tr class=\"table_header\"><th>Stream</th><th>Node</th><th>Today#</th><th>Total#</th></tr></thead>
        <tbody>" >>${HTML_FILE};
        while read line; do
                #echo ${line};
                IFS=$'\t' read -a array <<< "`echo "${line}" | cut -d$'\t' -f 1,2,3,4`";
                #echo " * " ${array[0]} ${array[1]} ${array[2]};
                if [[ ${array[2]} != 'discard_count*0' ]]; then
                        printf "%s %s %'d %'d\n" "`echo ${array[0]} | cut -d "*" -f 2`" "`echo ${array[1]} | cut -d "*" -f 2`" "`echo ${array[3]} | cut -d "*" -f 2`" "`echo ${array[2]} | cut -d "*" -f 2`" >> "${TEMP_MID}s";
                fi
        done < "${TEMP_MID}";
        #cat "${TEMP_MID}s";


        if [ -f "${TEMP_MID}s" ]; then
                sort -rn -k 3,4 "${TEMP_MID}s" > ${TEMP_MID};

                while read line; do
                        echo "<tr><td>"`echo $line | cut -d" " -f 1`"</td>" >> ${HTML_FILE};
                        echo "<td>"`echo $line | cut -d" " -f 2`"</td>" >> ${HTML_FILE};
                        echo "<td>"`echo $line | cut -d" " -f 3`"</td>" >> ${HTML_FILE};
                        echo "<td>"`echo $line | cut -d" " -f 4`"</td></tr>" >> ${HTML_FILE};
                done < "${TEMP_MID}";
        fi
        echo "</tbody></table></div>" >>${HTML_FILE};
}

write_rejects_html() {
        # Rejected ******************************************************************************
        grep "" ${RESULT_FILE} | cut -d$'\t' -f $STREAM_INDEX,$NODE_INDEX,$REJECTED_COUNT,$REJECTED_TODAY_COUNT > ${TEMP_MID};
        #cat ${TEMP_MID};
        if [ -f "${TEMP_MID}s" ]; then
                rm -f "${TEMP_MID}s";
        fi

        echo "<div class=\"normal_div\">
        <table class=\"table table-striped table-bordered table-hover\" id=\"reject_table\">
        <caption>Rejected CDR(s)</caption>
        <thead><tr class=\"table_header\"><th>Stream</th><th>Node</th><th>Today#</th><th>Total#</th></tr></thead>
        <tbody>" >>${HTML_FILE};
        while read line; do
                IFS=$'\t' read -a array <<< "`echo "${line}" | cut -d$'\t' -f 1,2,3,4`";
                #echo " * " ${array[0]} ${array[1]} ${array[2]} ${array[3]};
                if [[ ${array[2]} != 'reject_count*0' ]]; then
                        printf "%s %s %'d %'d\n" "`echo ${array[0]} | cut -d "*" -f 2`" "`echo ${array[1]} | cut -d "*" -f 2`" "`echo ${array[3]} | cut -d "*" -f 2`" "`echo ${array[2]} | cut -d "*" -f 2`" >> "${TEMP_MID}s";
                        #cat
                fi
        done < ${TEMP_MID};
        #cat "${TEMP_MID}s";


        if [ -f "${TEMP_MID}s" ]; then
                sort -rn -k 3,4 "${TEMP_MID}s" > ${TEMP_MID};

                while read line; do
                        echo "<tr><td>"`echo $line | cut -d" " -f 1`"</td>" >> ${HTML_FILE};
                        echo "<td>"`echo $line | cut -d" " -f 2`"</td>" >> ${HTML_FILE};
                        echo "<td>"`echo $line | cut -d" " -f 3`"</td>" >> ${HTML_FILE};
                        echo "<td>"`echo $line | cut -d" " -f 4`"</td></tr>" >> ${HTML_FILE};
                done < "${TEMP_MID}";
        fi
        echo "</tbody></table></div>" >>${HTML_FILE};
}

write_collectors_html() {
        # Collector Delay ******************************************************************************
        grep -P "type\*col\t" ${RESULT_FILE} | cut -d$'\t' -f $STREAM_INDEX,$NODE_INDEX,$EXECUTE_START_INDEX,$EXECUTE_END_INDEX,$LAST_PROCESS_INDEX,$DELAY_INDEX,$COLLECTED_TIME_INDEX > ${TEMP_MID};
        #echo "TEMP_MID: ";
        #cat ${TEMP_MID};

        if [ -f "${TEMP_MID}s" ]; then
                rm -f "${TEMP_MID}s";
        fi

        echo "<div class=\"bigger_div\">
        <table class=\"table table-striped table-bordered table-hover\" id=\"collector_delay\">
        <caption>Collectors Delay</caption>
        <thead><tr class=\"table_header\"><th>Stream</th><th>Node</th><th>Delay</th><th>Last Collected File</th></tr></thead>
        <tbody>" >>${HTML_FILE};
        while read line; do
                IFS=$'\t' read -a array <<< "`echo "${line}" | cut -d$'\t' -f 1,2,3,4,5,6,7`";
                #echo ${array[0]} ${array[1]} ${array[4]} ${array[5]}
                if [[ ${array[5]} != 'delay*0' ]]; then
                        if [ "${array[4]}" != "-" ] && [ "${array[4]}" != "" ]; then
                                array[4]=`echo ${array[4]} | cut -d ' ' -f 1`;
                        fi
                        printf "%s %s %s %s\n" "`echo ${array[0]} | cut -d "*" -f 2`" "`echo ${array[1]} | cut -d "*" -f 2`" "`echo ${array[4]} | cut -d "*" -f 2`" "`echo ${array[5]} | cut -d "*" -f 2`" >> "${TEMP_MID}s";
                fi
        done < "${TEMP_MID}";

        if [ -f "${TEMP_MID}s" ]; then

                #cat "${TEMP_MID}s";
                sort -rn -k 4 "${TEMP_MID}s" > "${TEMP_MID}";
                #cat "${TEMP_MID}";
                #sed -i -- 's/Collector/col/gi' ${TEMP_MID};

                while read line; do
                        echo "<tr><td>"`echo $line | cut -d" " -f 1`"</td>" >> ${HTML_FILE};
                        echo "<td>"`echo $line | cut -d" " -f 2`"</td>" >> ${HTML_FILE};

                        delay=`echo $line | cut -d" " -f 4`;
                        convert_seconds_to_DHM $delay;
                        echo "<td>${TMP}</td>" >> ${HTML_FILE};

                        echo "<td>"`echo $line | cut -d" " -f 3`"</td>" >> ${HTML_FILE};
                        echo "</tr>" >> ${HTML_FILE};
                done < "${TEMP_MID}";
        fi
        echo "</tbody></table></div>" >>${HTML_FILE};
}

write_distributors_html() {
        # Distributor Delay ******************************************************************************
        grep -P "type\*dis" ${RESULT_FILE} | cut -d$'\t' -f $STREAM_INDEX,$NODE_INDEX,$EXECUTE_START_INDEX,$EXECUTE_END_INDEX,$LAST_PROCESS_INDEX,$DELAY_INDEX,$COLLECTED_TIME_INDEX > ${TEMP_MID};


        if [ -f "${TEMP_MID}s" ]; then
                rm "${TEMP_MID}s";
        fi


        echo "<div class=\"bigger_div\">
        <table class=\"table table-striped table-bordered table-hover\" id=\"distributor_delay\">
        <caption>Distributors Delay</caption>
        <thead><tr class=\"table_header\"><th>Stream</th><th>Node</th><th>Delay</th><th>Last Distributed File</th></tr></thead>
        <tbody>" >>${HTML_FILE};
                while read line; do
                IFS=$'\t' read -a array <<< "`echo "${line}" | cut -d$'\t' -f 1,2,3,4,5,6,7`";
                #echo ${array[0]} ${array[1]} ${array[4]} ${array[4]}
                if [[ ${array[5]} != '' && ${array[5]} != 'delay*0' ]]; then
                        if [ "${array[4]}" != "-" ] && [ "${array[4]}" != "" ]; then
                                array[4]=`echo ${array[4]} | cut -d ' ' -f 1`;
                        fi
                        printf "%s %s %s %s\n" "`echo ${array[0]} | cut -d "*" -f 2`" "`echo ${array[1]} | cut -d "*" -f 2`" "`echo ${array[4]} | cut -d "*" -f 2`" "`echo ${array[5]} | cut -d "*" -f 2`" >> "${TEMP_MID}s";
                fi
        done < "${TEMP_MID}";

        if [ -f "${TEMP_MID}s" ]; then

                #cat "${TEMP_MID}s";
                sort -rn -k 4 "${TEMP_MID}s" > "${TEMP_MID}";
                #cat "${TEMP_MID}";


                #cat "${TEMP_MID}";
                while read line; do
                        #echo $line | cut -d" " -f 3;
                        echo "<tr><td>"`echo $line | cut -d" " -f 1`"</td>" >> ${HTML_FILE};
                        echo "<td>"`echo $line | cut -d" " -f 2`"</td>" >> ${HTML_FILE};

                        delay=`echo $line | cut -d" " -f 4`;
                        convert_seconds_to_DHM $delay;
                        echo "<td>${TMP}</td>" >> ${HTML_FILE};

                        # file name
                        echo "<td>"`echo $line | cut -d" " -f 3`"</td>" >> ${HTML_FILE};

                        echo "</tr>" >> ${HTML_FILE};
                done < "${TEMP_MID}";
        fi
        echo "</tbody></table></div>" >>${HTML_FILE};

}

write_stopped_node_html() {
        # Backlogs ******************************************************************************
        grep "" ${RESULT_FILE} | cut -d$'\t' -f $STREAM_INDEX,$NODE_INDEX,$STOPPED_INDEX > ${TEMP_MID};

        if [ -f "${TEMP_MID}s" ]; then
                rm -f "${TEMP_MID}s";
        fi
        echo "<div class=\"normal_div\">
        <table class=\"table table-striped table-bordered table-hover\" id=\"stopped_table\">
        <caption>Stopped Node(s)</caption>
        <thead><tr class=\"table_header\"><th>Stream</th><th>Node</th></tr></thead>
        <tbody>" >>${HTML_FILE};
        while read line; do
                IFS=$'\t' read -a array <<< "`echo "${line}" | cut -d$'\t' -f 1,2,3`";
                if [[ ${array[2]} == 'stopped*1' ]]; then
                        printf "%s %s %'d\n" "`echo ${array[0]} | cut -d "*" -f 2`" "`echo ${array[1]} | cut -d "*" -f 2`" >> "${TEMP_MID}s";   # "`echo ${array[2]} | cut -d "*" -f 2`"
                fi
        done < "${TEMP_MID}";

        if [ -f "${TEMP_MID}s" ]; then
                #cat "${TEMP_MID}s";
                sort -k 1 "${TEMP_MID}s" > ${TEMP_MID};
                #cat ${TEMP_MID};

                while read line; do
                        echo "<tr><td>"`echo $line | cut -d" " -f 1`"</td>" >> ${HTML_FILE};
                        echo "<td>"`echo $line | cut -d" " -f 2`"</td></tr>" >> ${HTML_FILE};
                        #echo "<td>STOP</td></tr>" >> ${HTML_FILE};
                done < "${TEMP_MID}";
        fi


        echo "</tbody></table></div>" >>${HTML_FILE};
}

write_failed_node_html() {
        # Backlogs ******************************************************************************
        grep "" ${RESULT_FILE} | cut -d$'\t' -f $STREAM_INDEX,$NODE_INDEX,$FAILED_INDEX,$FAILED_TIME_INDEX > ${TEMP_MID};

        if [ -f "${TEMP_MID}s" ]; then
                rm -f "${TEMP_MID}s";
        fi

        while read line; do
                IFS=$'\t' read -a array <<< "`echo "${line}" | cut -d$'\t' -f 1,2,3,4`";
                if [[ ${array[2]} == 'failed*1' ]]; then
                        printf "%s %s %s\n" "`echo ${array[0]} | cut -d "*" -f 2`" "`echo ${array[1]} | cut -d "*" -f 2`" "`echo ${array[3]} | cut -d "*" -f 2`" >> "${TEMP_MID}s";  # "`echo ${array[2]} | cut -d "*" -f 2`"
                fi
        done < "${TEMP_MID}";



        echo "<div class=\"normal_div\">
        <table class=\"table table-striped table-bordered table-hover\" id=\"failed_table\">
        <caption>Failed Node(s)</caption>
        " >>${HTML_FILE};

        if [ -f "${TEMP_MID}s" ]; then
                echo "<thead><tr class=\"table_header error\"><th>Stream</th><th>Node</th><th>Time</th></tr></thead>
        <tbody>" >> ${HTML_FILE};

                #cat "${TEMP_MID}s";
                sort -k 1 "${TEMP_MID}s" > ${TEMP_MID};
                #cat ${TEMP_MID};

                while read line; do
                        echo "<tr style=\"error\"><td>"`echo $line | cut -d" " -f 1`"</td>" >> ${HTML_FILE};
                        echo "<td>"`echo $line | cut -d" " -f 2`"</td>" >> ${HTML_FILE};
                        echo "<td>"`echo $line | cut -d" " -f 3,4`"</td></tr>" >> ${HTML_FILE};
                        #echo "<td>STOP</td></tr>" >> ${HTML_FILE};
                done < "${TEMP_MID}";
        fi


        echo "</tbody></table></div>" >>${HTML_FILE};
}

write_records_count_html() {
        # Records and files count ******************************************************************************
        grep "" ${RESULT_FILE} | cut -d$'\t' -f $STREAM_INDEX,$NODE_INDEX,$TYPE_INDEX,$RECORDS_INDEX,$FILES_INDEX > ${TEMP_MID};

        if [ -f "${TEMP_MID}s" ]; then
                rm "${TEMP_MID}s";
        fi


        sum_in=0;
        sum_f_in=0;
        sum_out=0;
        sum_f_out=0;
        while read line; do
                IFS=$'\t' read -a array <<< "`echo "${line}" | cut -d$'\t' -f 1,2,3,4,5`";
                #echo ${array[2]};

                if [[ "${array[2]}" == *"blt"* || "${array[2]}" == *"agr"* || "${array[2]}" == *"businesslogic"* ]]; then
                        #echo ${array[4]};
                        #echo ${array[3]};
                        if [[ "${array[3]}" != "records*" ]]; then
                                records=`echo ${array[3]} | cut -d "*" -f 2`;
                                if [[ "${records}" != "0" ]]; then
                                        #printf "%-25s%-30s%-19s%'-24d%'-28d\n" "`echo ${array[0]} | cut -d "*" -f 2`" "`echo ${array[1]} | cut -d "*" -f 2`" "`echo ${array[2]} | cut -d "*" -f 2`" "`echo ${array[4]} | cut -d "*" -f 2`" "`echo ${array[3]} | cut -d "*" -f 2`" >> "${TEMP_MID}s";
                                        sum_out=$(( records + sum_out ));
                                        sum_f_out=$(( sum_f_out + `echo ${array[4]} | cut -d "*" -f 2` ));
                                fi
                        fi
                elif [[ ${array[2]} == *"dec"* ]]; then
                        if [[ "${array[3]}" != "records*" ]]; then
                                records=`echo ${array[3]} | cut -d "*" -f 2`;
                                if [[ "${records}" != "0" ]]; then
                                        #printf "%-25s%-30s%-19s%'-24d%'-28d\n" "`echo ${array[0]} | cut -d "*" -f 2`" "`echo ${array[1]} | cut -d "*" -f 2`" "`echo ${array[2]} | cut -d "*" -f 2`" "`echo ${array[4]} | cut -d "*" -f 2`" "`echo ${array[3]} | cut -d "*" -f 2`" >> "${TEMP_MID}s";
                                        sum_in=$(( sum_in + records ));
                                        sum_f_in=$(( sum_f_in + `echo ${array[4]} | cut -d "*" -f 2` ));
                                fi
                        fi
                fi
        done < "${TEMP_MID}";


        echo "<div class=\"normal_div\">
        <table class=\"table table-striped table-bordered table-hover\" id=\"statistic_table\">
        <caption>Statistic</caption>
        <thead><tr class=\"table_header statistic_table\"><th>In/Out</th><th>File(s)</th><th>Record(s)</th></tr></thead>
        <tbody>" >>${HTML_FILE};

        printf "<tr class=\"table_summery\"><td>Input:</td><td>%'-15d</td><td>%'-20d</td></tr>\n" "${sum_f_in}" "${sum_in}" >> ${HTML_FILE};
        printf "<tr class=\"table_summery\"><td>Output:</td><td>%'-15d</td><td>%'-20d</td></tr>\n" "${sum_f_out}" "${sum_out}" >> ${HTML_FILE};

        #if [ -f "${TEMP_MID}s" ]; then
        #       sort -rn -k 4 "${TEMP_MID}s" > ${TEMP_MID};

        #       while read line; do
        #               echo "<tr><td>"`echo $line | cut -d" " -f 1`"</td>" >> ${HTML_FILE};
        #               echo "<td>"`echo $line | cut -d" " -f 2`"</td>" >> ${HTML_FILE};
        #               echo "<td>"`echo $line | cut -d" " -f 4`"</td>" >> ${HTML_FILE};
        #               echo "<td>"`echo $line | cut -d" " -f 5`"</td></tr>" >> ${HTML_FILE};
        #       done < "${TEMP_MID}";
        #fi

        echo "</tbody></table>" >>${HTML_FILE};


}

write_html_file() {

        if [ ! -f "${RESULT_FILE}" ]; then
                return;
        fi

        if [ -f "${HTML_FILE}" ]; then
                rm -f ${HTML_FILE};
        fi


        write_log "creating html file ...";

        echo "<!DOCTYPE html>
<html lang="en">
<head>
        <link href=\"monitor/bootstrap/css/bootstrap.min.css\" rel=\"stylesheet\">
        <meta http-equiv=\"refresh\" content=\"${REFRESH_TIME}\">
        <title>${TITLE}</title>
        <style>
        * { margin: 0; font-family: Verdana, Arial, Helvetica, sans-serif; }
        body { background-color: #949CA1; }
        /* #main { margin: 10px auto 10px auto; width: 1200px; } */
        #main { margin: 0 10px; }
        #main .normal_div, .bigger_div, .small_div {
                float: left;
                padding: 0px 10px 2px 5px;
                display: block;
                margin-right: 10px;
                border-color: #ddd;
                border-width: 2px;
                border-radius: 4px 4px 0 0;
                border-style: solid;
                -webkit-box-shadow: none;
                box-shadow: none;
                overflow: scroll;
                max-height:350px;
                min-height:350px;
                margin-bottom: 20px;
                max-width: 24%;
                width: 24%;
        }
        #main .bigger_div {
                #max-width: 500px;
                #width: 500px;
        }
        #main .small_div {
                max-width: 300px;
                width: 300px;
        }

        /* #FA9579 */
        #header { background-color: #8dc3f1; margin: 0 0 20px 0; text-align: center; padding-bottom: 8px; }

        h1 {
                font-size: 30px;
                /* margin-right: 20px;  */
                font-weight: bold;
                display:inline-block;
                line-height: 14px;
        }
        #menu { position: absolute; top: 10px; right: 20px; font-size: 15px; display:inline-block; }
        #menu a { text-decoration: underline; color: black; font-size: 18px; font-weight: bold; margin-right: 10px; }
        #menu a:hover { color: red; }

        #main table { background-color: #FFF; }
        #main table td { padding: 0px 5px; font-size: 20px;  }
        #main table th { font-size: 20px; }
        #main caption { font-weight: bold; font-size: 20px; color: #FFF;  }
        .table_header { background-color: #9ff78f; }
        .table_summery { background-color: #F0F07E !important; font-weight: bold; }
        #last_modified {
                position: fixed;
                z-index: 1000000;
                right: 10px;
                bottom: 10px;
                background-color: #192B3F;
                padding: 3px;
                color: #fff;
                font-weight: bold;
                font-size: 18px;
        }
        .error {
                background-color: red !important;
                font-weight: bold;
                color: white !important;
        }
        .statistic_table {
                background-color: blue !important;
                font-weight: bold;
                color: white !important;
        }
        </style>
</head>
<body>
<div id=\"main\">
        <div id=\"header\">
                <h1>${TITLE} - v${VERSION}</h1>
        </div>
        <div id=\"menu\">
                <a href=\"monitor/archive/\" target=\"_blank\">Archive</a>
                <a href=\"monitor/help.html\" target=\"_blank\">Help</a>
        </div>
        <span id=\"last_modified\">Created: `date +"%Y/%m/%d %H:%M:%S"`, Scheduled: ${CRONTAB_TIME}s</span>
" >>${HTML_FILE};

        write_log "write_failed_node_html";
        write_failed_node_html;

        write_log "write_stopped_node_html";
        write_stopped_node_html;

        write_log "write_buffers_html";
        write_buffers_html;

        write_log "write_discards_html";
        write_discards_html;

        write_log "write_rejects_html";
        write_rejects_html;

        write_log "write_collectors_html";
        write_collectors_html;

        write_log "write_distributors_html";
        write_distributors_html;

        write_log "write_records_count_html";
        write_records_count_html;


        #echo "<br style=\"clear: both;\" />" >>${HTML_FILE};
        #echo "<br style=\"clear: both;\" />" >>${HTML_FILE};



        echo "</div>
                <script src=\"monitor/bootstrap/js/jquery-3.1.1.slim.min.js\"></script>
                <!-- Include all compiled plugins (below), or include individual files as needed -->
                <script src=\"monitor/bootstrap/js/bootstrap.min.js\"></script>

                </body>
        </html>" >>${HTML_FILE};

        echo "done. `date +"%Y%m%d %H:%M:%S"`";


}

make_archive() {
        if [ -f $HTML_FILE ]; then
                #stat $HTML_FILE | grep
                modify_date=`date -r $HTML_FILE +%Y%m%d`;
                if [[ $modify_date != $1 ]]; then
                        write_log "This is first time today! Archive yesterday last file.";

                        month_dir="${ARCHIVE_PATH}"`date -r $HTML_FILE +%Y%m`;
                        #echo $month_dir;
                        if [ ! -d $month_dir ]; then
                                mkdir $month_dir;
                        fi

                        month_dir="${month_dir}"/`date -r $HTML_FILE +%d`".html";
                        #echo $month_dir;

                        sed -i -- 's/\"monitor\/bootstrap\/css\/bootstrap.min.css\"/\"..\/..\/..\/monitor\/bootstrap\/css\/bootstrap.min.css\"/gi' $HTML_FILE;
                        sed -i -- 's/href=\"monitor\/archive\/\"//gi' $HTML_FILE;

                        cp -p $HTML_FILE $month_dir;
                fi
        fi

        # update archive list

}




# get specific date
until_date=$(date +"%Y%m%d"); #echo $d
now=$(date +"%Y/%m/%d %H:%M:%S");
#col_printed=false;

echo "===============================";
date;


make_archive $until_date;
#exit;

##########################################################
################### Collect Information ##################
##########################################################
get_streams_detail $until_date;

count_backlogs;

process_trace_files $until_date ${AVG} ${ALL} ${SPECIFIC_DATE};

# save data to a file
write_stream_detail;


if [ -f ${RESULT_FILE} ]; then
        sed -i -- 's/HUAWEI/H/gi' ${RESULT_FILE};
        sed -i -- 's/NOKIA/N/gi' ${RESULT_FILE};
        sed -i -- 's/Huaw/H/gi' ${RESULT_FILE};
        sed -i -- 's/Distribute/dis/gi' ${RESULT_FILE};
        sed -i -- 's/distributor/dis/gi' ${RESULT_FILE};
        sed -i -- 's/Decoder/dec/gi' ${RESULT_FILE};
        sed -i -- 's/Reporter/Rep/gi' ${RESULT_FILE};
        sed -i -- 's/Loaders/Ldr/gi' ${RESULT_FILE};
        sed -i -- 's/loader/ldr/gi' ${RESULT_FILE};

        sed -i -- 's/tap3_outbound_NR/TOB_NR/g' ${RESULT_FILE}; # replace bound with nothing !
        sed -i -- 's/tap3_outbound/TOB/g' ${RESULT_FILE}; # replace bound with nothing !
        sed -i -- 's/tap3_inbound_IR_NR/TIB_IR_NR/g' ${RESULT_FILE}; # replace bound with nothing !
        sed -i -- 's/ldr_summary/ldr_sum/g' ${RESULT_FILE}; # replace bound with nothing !
        sed -i -- 's/nrtrde_outbound/NOB/g' ${RESULT_FILE}; # replace bound with nothing !
        sed -i -- 's/nrtrde_inbound/NIB/g' ${RESULT_FILE}; # replace bound with nothing !

        sed -i -- 's/bound/b/g' ${RESULT_FILE}; # replace bound with nothing !
        #sed -i -- 's/tap3_inb_//g' ${RESULT_FILE}; # replace bound with nothing !

        sed -i -- 's/ARCH/Arc/gi' ${RESULT_FILE};
        sed -i -- 's/Collector/col/gi' ${RESULT_FILE};
        sed -i -- 's/NATIONAL_FILTER/national_filter/gi' ${RESULT_FILE};
        sed -i -- 's/TAP3_OUT_NR _oader/TOB_NR_ldr/gi' ${RESULT_FILE};

        #sed -i -- 's/tap3/tap/gi' ${RESULT_FILE};


        write_html_file;


        write_log "copying html file to webserver ... ";
        cp $HTML_FILE $WEB_SERVER_PATH;
        echo "done.";

fi


