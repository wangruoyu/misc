
import os
import gzip
import pandas as pd

def datafile_transform(datadir,gzdatafilename,saveFileName):
    #'HiSeqV2.gz' --> cancerName='HiSeqV2'
    cancerName=gzdatafilename.split('.')[0]
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
    wf.write('@relation '+cancerName+'\n\n')
    #print(df.columns)
    for col in df.columns:
        #print(col)
        if col == "sample":
            #print(df[col])
            #print(type(df[col]))
            for index,feature in df[col].items():
                wf.write("@attribute "+feature+" real\n")
            wf.write("@attribute class {"+cancerName+",normal}\n\n")
            wf.write("@data\n")
        else:
            '''
            #TCGA-AB-2899-03
            #TCGA：Project, 所有TCGA样本名均以这个开头，标志
            #AB：Tissue source site，组织来源编码，如AB 表示来源于Washington University 急性髓细胞性白血病，A6就表示来源于Christiana Healthcare中心的结肠癌组织。更多编码所代表的意义详见：https://gdc.cancer.gov/resources-tcga-users/tcga-code-tables/tissue-source-site-codes
            #6650：Participant, 参与者编号
            #编号01~09表示肿瘤，10~19表示正常对照
            '''
            cn = col.split('-')
            if (int(cn[3])) <= 9:
                tag = cancerName
            else:
                tag = 'normal'
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
    gznames = []
    for filename in os.listdir(datadir):
        if filename.endswith('.gz'):
            gznames.append(filename)
    #gzdatafilename='HiSeqV2.gz'
    #saveFileName='HiSeqV2.arff'
    for gzdatafilename in gznames:
        saveFileName = gzdatafilename.replace('.gz','.arff')
        print("process %s and write to %s." %(gzdatafilename,saveFileName))
        datafile_transform(datadir,gzdatafilename,saveFileName)