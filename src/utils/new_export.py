import os, os.path, subprocess
import pandas as pd

def subTest():
  # create a dictionary of mdb filenames
  dimaDir = os.path.join(os.getcwd(),"dimas")
  filenames = {}
  prefix_count = 1
  targetDir = ''
  tableDict = {}

  for dima in os.listdir(dimaDir):
    if os.path.splitext(dima)[1]=='.mdb':
      
      filenames[f"dima_{prefix_count}"] = os.path.splitext(dima)[0]

  
  # run external export script to export tables contained inside mdb as csv
  path = os.path.join(os.getcwd(),"src","utils","export.sh")
  print(path)
  if path:
    p = subprocess.Popen(
      path, 
      stdin=subprocess.PIPE, 
      stdout=subprocess.PIPE,
      shell=True
      )
  print(p.communicate())

  # for each directory that contains csvs, create dictionary of dataframes
  # for key in filenames:
  #   targetDir = filenames[key].replace(" ","")
  #   targetDir = f"extracted_${targetDir}"
  #   joinedPath = os.path.join(dimaDir,targetDir)
  #   if os.path.exists(joinedPath):
  #     tempDir = os.path.join(dimaDir,targetDir)
  #     print(os.listdir(tempDir))
  #     for csv in os.listdir(tempDir):
       
  #       trimmedFilename = csv.replace(f"{targetDir}-",'')
  #       trimmedFilename = os.path.splitext(trimmedFilename)[0]
  #       tableDict[trimmedFilename] = pd.read_csv(os.path.join(joinedPath,csv))

  # print(tableDict)