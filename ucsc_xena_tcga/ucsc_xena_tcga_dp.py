
import os
import gzip
import csv
import pandas as pd

def cacheSubtypeNames(subtype_filepath):
    dictSubtypes={}
    '''
"","pan.samplesID","cancer.type","Subtype_mRNA","Subtype_DNAmeth","Subtype_protein","Subtype_miRNA","Subtype_CNA","Subtype_Integrative","Subtype_other","Subtype_Selected"
"1","TCGA-OR-A5J1","ACC","steroid-phenotype-high+proliferation","CIMP-high",NA,"miRNA_1","Quiet","COC3","C1A","ACC.CIMP-high"
'''
    with open(subtype_filepath,'r') as csvfile:
        reader = csv.reader(csvfile)
        rows= [row for row in reader]
    firstrow_skip=True
    for row in rows:
        if firstrow_skip:
            firstrow_skip = False
            continue
        else:
            dictSubtypes[row[1]]=row[2]

    return dictSubtypes

def datafile_transform(datadir,gzdatafilename,saveFileName,dictSubtypes):
    #'HiSeqV2.gz' --> cancerName='HiSeqV2'
    cancerName=gzdatafilename.split('.')[0]
    subtypeNames=set()
    for subtype in dictSubtypes.values():
        subtypeNames.add(subtype)
    # print(subtypeNames)
    with gzip.open(os.path.join(datadir,gzdatafilename), 'rt') as f:
        df = pd.read_csv(f, sep='\t')
        # print(df.info)
        '''
        file_content = f.readlines()
        print(type(file_content))
        print(len(file_content))
        first_line = True
        for line in file_content:
            list_line = line.split()
            if first_line:
                first_line = False
                print(list_line)
            else:
                pass
        '''
    #print(df.columns)
    '''ndf = pd.DataFrame(df.T,index=df.columns, columns=df.index)
    print(ndf.info)'''
    wf = open(os.path.join(datadir,saveFileName),'w')
    if (saveFileName.endswith(".arff")):
        wf.write('@relation '+cancerName+'\n\n') #需要生成arff文件格式
    #print(df.columns)
    for col in df.columns:
        #print(col)
        if col == "sample":
            if (saveFileName.endswith(".arff")):
                #需要生成arff文件格式
                #print(df[col])
                #print(type(df[col]))
                for index,feature in df[col].items():
                    wf.write("@attribute "+feature+" real\n")
                wf.write("@attribute class {"+cancerName)
                for subtype in subtypeNames:
                    wf.write(","+subtype)
                wf.write(",normal}\n\n")
                wf.write("@data\n")
        else:
            '''
            #TCGA-AB-2899-03
            #TCGA：Project, 所有TCGA样本名均以这个开头，标志
            #AB：Tissue source site，组织来源编码，如AB 表示来源于Washington University 急性髓细胞性白血病，A6就表示来源于Christiana Healthcare中心的结肠癌组织。更多编码所代表的意义详见：https://gdc.cancer.gov/resources-tcga-users/tcga-code-tables/tissue-source-site-codes
            #2899：Participant, 参与者编号
            #编号01~09表示肿瘤，10~19表示正常对照
            '''
            cn = col.split('-')
            #文件处理代码再修改下，cancer是0，正常是1，不用文字
            #cancel类型先查询子类型，查询不到或查询的名字为"NA"再使用文件名的cancel类型
            if (int(cn[3])) <= 9:
                sampleName=cn[0]+'-'+cn[1]+'-'+cn[2]
                if sampleName in dictSubtypes:
                    tag=dictSubtypes[sampleName]
                else:
                    tag=cancerName
            else:
                tag = 'NORMAL'
            values=''
            for index,value in df[col].items():
                if values == '':
                    values = str(value)
                else:
                    values = values + "," + str(value)
            #print(values+","+tag)
            wf.write(values+","+tag+"\n")
        #print(df[col])

    wf.close()

if __name__=='__main__':
    datadir='C:\\rywang\\lz\\tcga_dataprocess'
    subtype_fileName='subtypes.csv'
    dictSubtypes = cacheSubtypeNames(os.path.join(datadir,subtype_fileName))
    #print(dictSubtypes)
    gznames = []
    for filename in os.listdir(datadir):
        if filename.endswith('.gz'):
            gznames.append(filename)
    #gzdatafilename='HiSeqV2.gz'
    #saveFileName='HiSeqV2.arff'
    print("datadir: %s" %(datadir))
    for gzdatafilename in gznames:
        #saveFileName = gzdatafilename.replace('.gz','.arff')
        saveFileName = gzdatafilename.replace('.gz','.csv')
        print("process %s and write to %s." %(gzdatafilename,saveFileName))
        datafile_transform(datadir,gzdatafilename,saveFileName,dictSubtypes)