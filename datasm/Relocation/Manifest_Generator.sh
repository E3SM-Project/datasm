#!/bin/bash

manifest_spec=$1

thisuser=`whoami`
ops_root=`$DSM_GETPATH USER_ROOT`/$thisuser/Operations


# Load the local Root Paths

RootPathsFile=/p/user_pub/e3sm/staging/.dsm_root_paths

declare -A RootPaths

for aline in `cat $RootPathsFile`; do
    tag=`echo $aline | cut -f1 -d:`
    val=`echo $aline | cut -f2 -d:`
    RootPaths[$tag]=$val
done

# ====================================

verbose=0

for aline in `cat $manifest_spec`; do

    if [ $verbose -eq 1 ]; then
        echo "PROCESSING SPEC LINE:  $aline"
    fi

    section=`echo $aline | cut -f1 -d,`
    roottag=`echo $aline | cut -f2 -d,`
    tailtyp=`echo $aline | cut -f3 -d,`
    srcpath=`echo $aline | cut -f4 -d,`
    srcspec=`echo $aline | cut -f5 -d,`

    if [ $section == "COMMON" ]; then

        if [ -z $srcpath ]; then
            srcpath=${RootPaths[$roottag]}
        fi

        if [ $tailtyp == "FILE" ]; then
            echo "$section,$roottag,FILE,$srcpath,$srcspec"
        elif [ $tailtyp == "DIRNAME" ]; then
            echo "$section,$roottag,DIRNAME,,$srcspec"
        elif [ $tailtyp == "TYPE" ]; then
            if [ $srcspec == "REGFILES" ]; then
                for item in `ls $srcpath`; do
                    if [ -f $srcpath/$item ]; then
                        echo "$section,$roottag,FILE,$srcpath,$item"
                    fi
                done
            elif [ $srcspec == "DIRNAMES" ]; then
                for item in `ls $srcpath`; do
                    if [ -d $srcpath/$item ]; then
                        echo "$section,$roottag,DIRNAME,,$item"
                    fi
                done
            fi
        elif [ $tailtyp == "GLOB" ]; then
            for item in `ls $srcpath/$srcspec | cut -f1 -d:`; do
                item_found=`basename $item`
                if [ -f $item ]; then
                    echo "$section,$roottag,FILE,$srcpath,$item_found"
                elif [ -d $item ]; then
                    echo "$section,$roottag,DIRNAME,,$item_found"
                fi
            done

        elif [ ${tailtyp:0:6} == "PATHTO" ]; then
            tailtyp=${tailtyp:7}

            extpath=`dirname $srcspec`
            fullpath="$srcpath/$extpath"
            srcspec=`basename $srcspec`
            
            if [ $tailtyp == "FILE" ]; then
                echo "$section,$roottag,PATHTO_FILE,$srcpath,$extpath/$srcspec"
            elif [ $tailtyp == "DIRNAME" ]; then
                echo "$section,$roottag,PATHTO_DIRNAME,$srcpath,$srcspec"
            elif [ $tailtyp == "TYPE" ]; then
                if [ $srcspec == "REGFILES" ]; then
                    for item in `ls $fullpath`; do
                        if [ -f $fullpath/$item ]; then
                            echo "$section,$roottag,PATHTO_FILE,$srcpath,$extpath/$item"
                        fi
                    done
                elif [ $srcspec == "DIRNAMES" ]; then
                    for item in `ls $fullpath`; do
                        if [ -d $fullpath/$item ]; then
                            echo "$section,$roottag,PATHTO_DIRNAME,$srcpath,$extpath/$item"
                        fi
                    done
                fi
            elif [ $tailtyp == "GLOB" ]; then
                for item in `ls $fullpath/$srcspec | cut -f1 -d:`; do
                    item_found=`basename $item`
                    if [ -f $item ]; then
                        echo "$section,$roottag,PATHTO_FILE,$srcpath,$extpath/$item_found"
                    elif [ -d $item ]; then
                        echo "$section,$roottag,PATHTO_DIRNAME,$srcpath,$extpath/$item_found"
                    fi
                done
            fi
        fi
    elif [ $section == "USEROP" ]; then

        if [ -z $srcpath ]; then
            srcpath=$ops_root/$roottag          # i.e. [USER_ROOT]/$thisuser/Operations / 0_Documentation
        fi

        if [ $tailtyp == "FILE" ]; then
            echo "$section,$roottag,FILE,$srcpath,$srcspec"
        elif [ $tailtyp == "DIRNAME" ]; then
            echo "$section,$roottag,DIRNAME,,$srcspec"

        elif [ $tailtyp == "TYPE" ]; then
            if [ $srcspec == "REGFILES" ]; then
                for item in `ls $srcpath`; do
                    if [ -f $srcpath/$item ]; then
                        echo "$section,$roottag,FILE,$srcpath,$item"
                    fi
                done
            elif [ $srcspec == "DIRNAMES" ]; then
                for item in `ls $srcpath`; do
                    if [ -d $srcpath/$item ]; then
                        echo "$section,$roottag,DIRNAME,,$item"
                    fi
                done
            fi
        elif [ $tailtyp == "GLOB" ]; then
            for item in `ls $srcpath/$srcspec | cut -f1 -d:`; do
                item_found=`basename $item`
                if [ -f $item ]; then
                    echo "$section,$roottag,FILE,$srcpath,$item_found"
                elif [ -d $item ]; then
                    echo "$section,$roottag,DIRNAME,,$item_found"
                fi
            done

        elif [ ${tailtyp:0:6} == "PATHTO" ]; then
            tailtyp=${tailtyp:7}

            extpath=`dirname $srcspec`
            fullpath="$srcpath/$extpath"
            srcspec=`basename $srcspec`
            
            if [ $tailtyp == "FILE" ]; then
                echo "$section,$roottag,PATHTO_FILE,$srcpath,$extpath/$srcspec"
            elif [ $tailtyp == "DIRNAME" ]; then
                echo "$section,$roottag,PATHTO_DIRNAME,$srcpath,$srcspec"
            elif [ $tailtyp == "TYPE" ]; then
                if [ $srcspec == "REGFILES" ]; then
                    for item in `ls $fullpath`; do
                        if [ -f $fullpath/$item ]; then
                            echo "$section,$roottag,PATHTO_FILE,$srcpath,$extpath/$item"
                        fi
                    done
                elif [ $srcspec == "DIRNAMES" ]; then
                    for item in `ls $fullpath`; do
                        if [ -d $fullpath/$item ]; then
                            echo "$section,$roottag,PATHTO_DIRNAME,$srcpath,$extpath/$item"
                        fi
                    done
                fi
            elif [ $tailtyp == "GLOB" ]; then
                for item in `ls $fullpath/$srcspec | cut -f1 -d:`; do
                    item_found=`basename $item`
                    if [ -f $item ]; then
                        echo "$section,$roottag,PATHTO_FILE,$srcpath,$extpath/$item_found"
                    elif [ -d $item ]; then
                        echo "$section,$roottag,PATHTO_DIRNAME,$srcpath,$extpath/$item_found"
                    fi
                done
            fi

        fi

    fi
        
done

exit 0

