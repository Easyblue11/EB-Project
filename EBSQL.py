# Author: EasyBlue
# Time: 2022.3.20-21:00
import os
import time
import threading
"""
# 创建实例后必须需要调用Init(Rules:dict))函数
# Rules为初始化文件存储的格式
# 其余就是Add(list) Delete(dict) Change(dict,dict) Find(dict)
# 还有Print()调试时使用
"""



class SQL:
    def __init__(self):
        self.FilePath = "EBSQL.lib"
        self.mutex = threading.Lock()
        self.SQLDict = dict()
        self.Count = 0
        self.Rules = dict()
        
    
    # 退出自动保存
    def __del__(self):
        self.CombineDict()
        self.Write()


    # 获取文件大小
    def GetFileSize(self) -> int:
        return os.path.getsize(self.FilePath)


    # 创建文件
    def InitCreat(self) -> None:
        if not os.path.exists(self.FilePath):
            with open(self.FilePath,"w",encoding="utf-8") as f:
                f.write("")
        

    # 线程锁
    def __ThreadLock(Func):
        def Temp(self):
            self.mutex.acquire()
            #print("asd")
            Func(self)
            self.mutex.release()
        return Temp


    # 读取
    @__ThreadLock
    def InitRead(self) -> None:
        Content = str()
        with open(self.FilePath,"r",encoding="utf-8") as f:
            Content = f.read()
        if Content == str():
            with open(self.FilePath,"w",encoding="utf-8") as f:
                f.write(str(self.SQLDict))
        else:
            self.SQLDict = eval(Content)
            if self.SQLDict["Rules"] != dict():
                self.Rules = self.SQLDict["Rules"]
            self.Count = self.SQLDict["Count"]


    # 写入文件
    @__ThreadLock
    def Write(self) -> None:
        with open(self.FilePath,"w",encoding="utf-8") as f:
            f.write(str(self.SQLDict))


    # 初始化数值
    def InitData(self,Rules:dict) -> None:
        """ Rules = {"名字":"string","属性":"list"}
            支持格式 : num string list dict
            标签不能有id
        """
        for eachrules in Rules:
            if Rules[eachrules] not in ['num', 'string', 'list', 'dict']:
                raise "Unknown Type,Type ->(num, string, list, dict)"
            if eachrules == "id":
                raise "Tags cannot have 'id'"
        self.Rules = Rules
        self.Count = 0
        self.SQLDict = {"Rules":self.Rules,"Count":self.Count,"Main":list()}


    # Init
    def Init(self,Rules:dict = dict()) -> None:
        self.InitData(Rules)
        self.InitCreat()
        self.InitRead()
        self.AutoSave()
    

    # 组合文件
    @__ThreadLock
    def CombineDict(self) -> None:
        self.SQLDict["Rules"] = self.Rules
        self.SQLDict["Count"] = self.Count


    # 更改Rules
    def ChangeRules(self,NewRules:dict) -> None:
        self.Rules = NewRules
        self.CombineDict()


    # 自动保存
    def AutoSave(self) -> None:
        def Func_AutoSave(self):
            while True:
                self.Write()
                self.InitRead()
                time.sleep(10)
                
        T = threading.Thread(target=self.Write)
        T.start()
        
    
    # 验证是否合法
    def __CheckRules(self,CheckList) -> bool:
        RetBool = True
        if len(CheckList) == len(self.Rules):
            Count = 0
            for Keys in self.Rules:
                Temp = self.Rules[Keys]
                if Temp == "string":
                    if not type(CheckList[Count]) == type(str()):
                        RetBool = False
                elif Temp == "list":
                    if not type(CheckList[Count]) == type(list()):
                        RetBool = False
                elif Temp == "dict":
                    if not type(CheckList[Count]) == type(dict()):
                        RetBool = False
                elif Temp == "num":
                    if not type(CheckList[Count]) == type(int()) or not CheckList[Count] == type(float()):
                        RetBool = False
                Count += 1
        else:
            RetBool = False
        return RetBool


    # 增加
    def Add(self,ContentList:list) -> bool:
        self.mutex.acquire()            
        if self.__CheckRules(ContentList):
            try:
                self.SQLDict["Main"].append({"id":self.Count,"Content":ContentList})
                self.Count += 1
                self.mutex.release()
                return True
            except:
                self.mutex.release()
                return False
        else:
            self.mutex.release()
            return False


    # 更具查找对象返回查找索引
    def __FindIndex(self,FindDict) ->list:
        RetList = list()
        for Keys in FindDict:
            if Keys != "id":
                try:
                    index = list(self.Rules.keys()).index(Keys)
                except:
                    return list()
                if index!= -1:
                    RetList.append(index)
                else:
                    return list()
        return RetList


    # 优先级判断返回
    def __Priority(self,FindDict,ContentList,FindIndexList)->int:
        P = 0
        Flag = 0
        FindList = list(FindDict.values())
        for count in FindIndexList:
            Content = ContentList[count]
            FindContent = FindList[Flag]
            if FindContent == Content:
                P += 2
            elif FindContent in Content:
                P += 1
            else:
                return 0
            Flag += 1
        return P

    
    # 返回优先级id List
    def __PriorityForId(self,PDict) -> list:
        ValueList = list(PDict.values())
        MaxValue = max(ValueList)
        if MaxValue != 0:
            RetIdList = list()
            for id in PDict:
                if PDict[id] == MaxValue:
                    RetIdList.append(id)
            return RetIdList
        else:
            return list()


    # 查找
    def Find(self,FindDict:dict) -> list:
        """
            Dict = {'id':?,'name':''}
        """
        if "id" in FindDict:
            for EachDict in self.SQLDict['Main']:
                if EachDict['id'] == FindDict['id']:
                    return [EachDict]
            return list()
        else:
            FindIndexList = self.__FindIndex(FindDict)
            if self.SQLDict['Main'] != list() and FindIndexList != list():
                PDict = dict() # id:P
                for EachDict in self.SQLDict['Main']:
                    p = self.__Priority(FindDict,EachDict['Content'],FindIndexList)
                    PDict[EachDict['id']] = p
                IDList = self.__PriorityForId(PDict)
                RetList = list()
                if IDList != list():
                    for EachDict in self.SQLDict['Main']:
                        if EachDict["id"] in IDList:
                            RetList.append(EachDict)
                    return RetList
                else:
                    return list()
            else:
                return list()
            

    # 匹配删除
    def Delete(self,FindDict) -> bool:
        self.mutex.acquire()
        DeleteList = self.Find(FindDict)
        if DeleteList != list():
            for EachDelete in DeleteList:
                print(EachDelete)
                try:
                    self.SQLDict["Main"].remove(EachDelete)
                except Exception as ret:
                    print(ret)
                    self.mutex.release()
                    self.CombineDict()
                    self.Write()
                    raise 'An error occurred during deletion.'
            self.mutex.release()
            return True
        else:
            self.mutex.release()
            return False


    # 更改
    def Change(self,FindDict:dict,ChageItem:dict) -> bool:
        FindList = self.Find(FindDict)
        if FindList != list():
            IDList = list()
            IndexList = self.__FindIndex(ChageItem)
            if IndexList == list():
                return False
            ChageList = list(ChageItem.values())
            for IDDict in FindList:
                IDList.append(IDDict["id"])
            for EachDict in self.SQLDict["Main"]:
                if EachDict['id'] in IDList:
                    Flag = 0
                    for i in IndexList:
                        EachDict['Content'][i] = ChageList[Flag]
                        Flag += 1
            return True
        else:
            return False

    
    # 打印
    def Print(self) -> None:
        print("%-20s" % ("id:num"+str(self.Count)),end="")
        for Key in self.Rules:
            print("%-20s" % (str(Key)+":"+self.Rules[Key]),end="")
        print("")
        for Dict in self.SQLDict['Main']:
            print("%-20s" % Dict["id"],end="")
            for eachone in Dict["Content"]:
                print("%-20s" % eachone,end="")
            print()
        
        
    # 获取规则
    def PrintRules(self) -> None:
        print(self.Rules)


