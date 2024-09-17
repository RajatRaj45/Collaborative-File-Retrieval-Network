// Socket for listening to receive requests     REQ -> Processing -> Port assign -> Send ID & IP LIST -> Fork -> Socket ON ( IP List & SocketID )
// Socket for registration & file updates requests   REG -> Registration    FUP -> File Update

// Socket for transaction  accept x1  listen CON -> process  NACK


/*
    DATASTRUCTURES:    
    map<IP, socketID> register : Every peer at the time of reg
    map<file, vector<IP>> file_system : Update everytime FUP received
    map<IP_receiver, vector<pair<IP_peer,SocketID>>> transaction
*/

// PORT NUMBER 8080 -> REC
// PORT NUMBER 6969 -> REG & FUP

#include <stdio.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <string.h>
#include <fstream>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <bits/stdc++.h>
#include <unistd.h>
#include <map>
#include <vector>

using namespace std;

#define REG_PORT 6969
#define REQ_PORT 8080
#define PEER_PORT 9000
#define ll long long

vector<string> split(string s){
    // s -> "FILENAME1 FILENAME2 ...."
    vector<string> res;
    string temp = "";
    for(int i=0;i<s.size();i++){
        if(s[i]==' '){
            res.push_back(temp);
            temp = "";
        }
        else{
            temp.push_back(s[i]);
        }
    }
    if(temp!=""){
        res.push_back(temp);
    }
    return res;
}


void updateDataStructures(vector<string> &regis, map<string, vector<string>> &file_system){
    //FILE NAMES
    vector<string> newreg;
    map<string,vector<string>> new_file_system;
    fstream regfile;
    regfile.open("Register.txt", ios::in);
    string x;
    while(getline(regfile,x)){
        newreg.push_back(x);
    }
    regis = newreg;
    regfile.close();

    fstream filesys;
    filesys.open("FileSystem.txt",ios::in);
    string x1;
    while(getline(filesys,x1)){
        vector<string> temp = split(x1);
        for(int i=1;i<temp.size();i++){
            new_file_system[temp[0]].push_back(temp[i]);
        }
    }
    file_system = new_file_system;
    filesys.close();
}

void writeEntryRegister(string IP){
    //IP1
    //IP2
    //IP3
    //....

    //OPEN FILE IN APPEND MODE
    //WRITE IP

    fstream checkfile;
    checkfile.open("Register.txt", ios::in);
    string ip_reg;
    while(getline(checkfile,ip_reg)){
        if(ip_reg==IP){
            return;
        }
    }

    ofstream regfile;
    regfile.open("Register.txt", ios::app);
    regfile<<IP<<endl;
    regfile.close();
}

void writeEntryFileSystem(string fileID, string IP){
    // FILEID1 ip1 ip2 ip3 .....
    // FILEID2 ip1 ip2 ip3 .....
    // FILEID3 ip1 ip2 ip3 .....
    
    //OPEN FILE IN READ MODE
    //MAP<string,vector<string>> made
    //ADD ENTRY IN MAP
    //OPEN FILE IN WRITE MODE
    //WRITE MAP    

    fstream file;
    file.open("FileSystem.txt",ios::in);
    string entry;
    string modified_entry = fileID+" "+IP;
    string entry_delete = "";
    while(getline(file,entry)){
        if(entry.size() < fileID.size()){
            continue;
        }
        if(entry.substr(0,fileID.size()) == fileID){
            entry_delete = entry;
            entry = entry + " " + IP;
            modified_entry = entry;
            break;
        }
    }
    file.close();

    if(entry_delete!=""){
        vector<string> x = split(entry_delete);
        for(int i=1;i<x.size();i++){
            if(x[i]==IP){
                return;
            }
        }
    }

    if(entry_delete != ""){
        fstream file;
        file.open("FileSystem.txt",ios::in);
        ostringstream text;
        text << file.rdbuf();
        string str = text.str();
        size_t pos = str.find(entry_delete);
        str.replace(pos,string(entry_delete).length(),modified_entry);
        file.close();
        ofstream out_file("FileSystem.txt");
        out_file << str;
        out_file.close();
    }
    else{
        fstream nfile("FileSystem.txt", ios::app);
        nfile << modified_entry <<endl;
        nfile.close();
    }
    
}

int readFreePortNumber(){
    //READ FILE
    // return 4000;

    fstream file("FreePort.txt");
    ostringstream num;
    num << file.rdbuf();
    file.close();
    string n = num.str();
    if(n==""){
        n = "4000";
    }
    int res = stoi(n);
    int writenum = res+1;
    string writes = to_string(writenum);
    fstream file1("FreePort.txt", ios::out);
    file1<<writes;
    file1.close();
    return res;
}

string ipconvert(ll ip){
    string s = "";
    for(ll i = 0;i<32;i++){
        if(ip%2==1){
            s.push_back('1');
        }
        else{
            s.push_back('0');
        }
        ip/=2;
    }
    reverse(s.begin(),s.end());
    string res = "";
    for(int i=3;i>=0;i--){
        ll ans = 0;
        for(int j=0;j<8;j++){
            ans *= 2;
            if(s[i*8+j]=='1'){
                ans+=1;
            }
        }
        res = res + to_string(ans);
        if(i>0){
            res.push_back('.');
        }
    }
    return res;
}

int main(){
    vector<string> reg; // IP -> SockID
    map<string, vector<string>> file_system; // FileID -> {Ip1, Ip2, ....}
    // map<string, vector<pair<string,int>>> transaction; // Ip_receiver -> {(IP_peer1, SockIDPeer1),....}
    
    //Remove Existing Files
    remove("Register.txt");
    remove("FileSystem.txt");
    remove("FreePort.txt");


    if(fork()==0){
        //REG&FUP
        
        //Create REG and FUP Socket
        struct sockaddr_in servaddr, cliaddr;
        int clilen, connfd, listenfd;
        listenfd = socket(AF_INET, SOCK_STREAM, 0);
        bzero(&servaddr, sizeof(servaddr));
        servaddr.sin_family = AF_INET;
        servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
        servaddr.sin_port = htons(REG_PORT);
        bind(listenfd, (struct sockaddr *)&servaddr, sizeof(servaddr));
        listen(listenfd,5);


        char msg[1000];
        for(;;){
            bzero(msg,sizeof(msg));
            clilen = sizeof(cliaddr);
            connfd = accept(listenfd, (struct sockaddr *)&cliaddr, (socklen_t*)&clilen);
            read(connfd,msg,sizeof(msg));
            string s(msg);
            if(s.substr(0,3)=="REG"){
                //REG FileName1 FileName2 .....
                
                //Retrieve IP of peer
                ll ip_int = cliaddr.sin_addr.s_addr;
                string ip_client = ipconvert(ip_int);
                cout<<"Registration of "<<ip_client<<" successful!\n";
                writeEntryRegister(ip_client);
                vector<string> files = split(s.substr(4));
                for(auto file:files){
                    writeEntryFileSystem(file,ip_client);
                }
                
                // Send ACK
                char msg[100];
                bzero(msg, sizeof(msg));
                string ack = "ACK "+ ip_client;
                for(int i=0;i<ack.size();i++){
                    msg[i] = ack[i];
                }
                write(connfd,msg,sizeof(msg));
            
            }
            
            else if(s.substr(0,3)=="FUP"){
                //FUP FileName1 FileName2 ....

                //Retrieve IP of peer
                ll ip_int = cliaddr.sin_addr.s_addr;
                string ip_client = ipconvert(ip_int);
                vector<string> files = split(s.substr(4));
                for(auto file:files){
                    writeEntryFileSystem(file,ip_client);
                }
                
                // Send ACK
                char msg[100];
                bzero(msg, sizeof(msg));
                string ack = "ACK";
                for(int i=0;i<ack.size();i++){
                    msg[i] = ack[i];
                }
                write(connfd,msg,sizeof(msg));
            
            }
            else{
                //INVALID MESSAGE RECEIVED
                char msg[100];
                bzero(msg, sizeof(msg));
                string error = "INVALID MESSAGE";
                for(int i=0;i<error.size();i++){
                    msg[i] = error[i];
                }
                write(connfd,msg,sizeof(msg));
            }
        }
    }
    else{
        //REQUEST PORT

        //CREATING REQUEST SOCKET
        struct sockaddr_in servaddr, cliaddr;
        int clilen, connfd, listenfd;
        listenfd = socket(AF_INET, SOCK_STREAM, 0);
        bzero(&servaddr, sizeof(servaddr));
        servaddr.sin_family = AF_INET;
        servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
        servaddr.sin_port = htons(REQ_PORT);
        bind(listenfd, (struct sockaddr *)&servaddr, sizeof(servaddr));
        listen(listenfd,5);


        char msg[1000];
        for(;;){
            bzero(msg,sizeof(msg));
            clilen = sizeof(cliaddr);
            connfd = accept(listenfd, (struct sockaddr *)&cliaddr, (socklen_t*)&clilen);
            //CREATE NEW PROCESS FOR EACH REQUEST
            if(fork()==0){
                //REQ FileName
                close(listenfd);
                read(connfd, msg, sizeof(msg));
                string s(msg);
                s = s.substr(0,s.size()-1);
                if(s.substr(0,3)!="REQ"){
                    bzero(msg,sizeof(msg));
                    string rt = "INVALID MESSAGE!";
                    for(int i=0;i<rt.size();i++){
                        msg[i] = rt[i];
                    }
                    write(connfd,msg,sizeof(msg));
                    close(connfd);
                    exit(0);
                }
                // WRITE IP LIST & PORT NUMBER
                updateDataStructures(reg, file_system);
                string filename = s.substr(4);
                vector<string> iplist = file_system[s.substr(4)];
                if(iplist.size()==0){
                    //Requested File does not exist on the network
                    bzero(msg,sizeof(msg));
                    string rt = "DET NA";
                    for(int i=0;i<rt.size();i++){
                        msg[i] = rt[i];
                    }
                    write(connfd,msg,sizeof(msg));
                    close(connfd);
                    exit(0);
                }

                //SEND IP LIST
                int portno = readFreePortNumber();
                string res = "DET ";
                for(int i=0;i<iplist.size();i++){
                    res = res + iplist[i] + " ";
                }
                res = res + to_string(portno);
                bzero(msg,sizeof(msg));
                for(int i=0;i<res.size();i++){
                    msg[i] = res[i];
                }
                write(connfd,msg,sizeof(msg));
                // CLOSE THIS SOCKET
                close(connfd);
                
                
                // OPEN NEW SOCKET WITH NEW PORT NUMBER
                listenfd = socket(AF_INET, SOCK_STREAM, 0);
                bzero(&servaddr, sizeof(servaddr));
                servaddr.sin_family = AF_INET;
                servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
                servaddr.sin_port = htons(portno);
                bind(listenfd, (struct sockaddr *)&servaddr, sizeof(servaddr));
                listen(listenfd,1);
                bzero(msg,sizeof(msg));
                clilen = sizeof(cliaddr);
                connfd = accept(listenfd, (struct sockaddr *)&cliaddr, (socklen_t*)&clilen);
                
                //READ CON Message from RECEIVER
                read(connfd,msg,sizeof(msg));
                ll ip_rec_int = cliaddr.sin_addr.s_addr;
                string ip_rec = ipconvert(ip_rec_int);
                //CON numports port1 ip_peer1 port2 ip_peer2 ....
                vector<pair<int,string>> transaction;
                string st(msg);
                
                if(st.substr(0,3)!="CON"){
                    cout<<"INVALID MESSAGE AT CONFIRMATION STAGE!\n\n";
                }
                st = st.substr(4);
                vector<string> parsed_message = split(st);
                ll numports;
                pair<int,string> p;
                for(int i = 0; i<parsed_message.size();i++){
                    if(i==0){
                        numports = stoi(parsed_message[i]);
                    }
                    else{
                        if(i%2==0){
                            //IP
                            p.second = parsed_message[i];
                            transaction.push_back(p);
                        }
                        else{
                            //PORT
                            p.first = stoi(parsed_message[i]);
                        }
                    }
                }
                //SEND MESSAGE TO ALL PEERS
                int counter = 0;
                for(auto p:transaction){
                    if(fork()==0){
                        close(connfd);
                        close(listenfd);
                        string ip_peer = p.second;
                        int port_to_be_sent = p.first;
                        //CREATE TCP SOCKET
                        
                        listenfd = socket(AF_INET, SOCK_STREAM, 0);
                        bzero(&servaddr, sizeof(servaddr));
                        servaddr.sin_family = AF_INET;
                        servaddr.sin_addr.s_addr = inet_addr(ip_peer.c_str());
                        servaddr.sin_port = htons(PEER_PORT);
                        if(connect(listenfd, (struct sockaddr*)&servaddr, sizeof(servaddr))<0){
                            exit(0);
                        }
                        // MESSAGE FORMAT: TASK FILE_NAME NUM_ASSIGNED NUM_PORTS IP_RECEIVER PORT_NUMBER
                        string message = "TASK "+filename+" "+to_string(counter)+" "+to_string(numports)+" "+ip_rec+" "+to_string(port_to_be_sent);
                        char task_mes[1000];
                        bzero(task_mes, sizeof(task_mes));
                        for(int i=0;i<message.size();i++){
                            task_mes[i] = message[i];
                        }
                        write(listenfd,task_mes,sizeof(task_mes));
                        //************************
                        exit(0);
                    }    
                    counter++;
                }    
                //WAIT FOR ACK
                bzero(msg,sizeof(msg));
                read(connfd,msg,sizeof(msg));
                string msg_ack(msg);
                if(msg_ack == "ACK"){
                    cout<<"DATA RECEIVED BY RECEIVER SUCCESSFULLY!\n";
                    close(connfd);
                    exit(0);
                }
                else{
                    //NAK PORT1 PORT2 ...
                    vector<pair<int,int>> failed_ports; //PORT & OFFSET
                    msg_ack = msg_ack.substr(4);
                    vector<string> temp = split(msg_ack);
                    for(auto s:temp){
                        int port_f = stoi(s);
                        for(int i=0;i<transaction.size();i++){
                            if(transaction[i].first == port_f){
                                failed_ports.push_back({port_f,i});
                                break;
                            }
                        }
                    }

                    //ALREADY TESTED PREFERRED
                    if(failed_ports.size()*2 <= transaction.size()){
                        vector<pair<int, pair<string, int>>> nack_trans; // PORT to be sent, IP of peer , Offset
                        int count = 0;
                        for(int i=0;i<transaction.size();i++){
                            bool isFailed = 0;
                            for(auto p:failed_ports){
                                if(transaction[i].first == p.first){
                                    isFailed = 1;
                                    break;
                                }
                            }
                            if(isFailed){
                                continue;
                            }
                            nack_trans.push_back({failed_ports[count].first, {transaction[i].second, failed_ports[count].second}});
                            count++;
                            if(count>=failed_ports.size()){
                                break;
                            }
                        }

                        //RDET IP1 IP2 IP3 .....
                        string message_rdet = "RDET";
                        for(auto el:nack_trans){
                            message_rdet = message_rdet + " " + el.second.first;
                        }
                        char rdet_mes[1000];
                        bzero(rdet_mes, sizeof(rdet_mes));
                        for(int i=0;i<message_rdet.size();i++){
                            rdet_mes[i] = message_rdet[i];
                        }
                        write(connfd, rdet_mes, sizeof(rdet_mes));
                        for(auto p:nack_trans){
                            if(fork()==0){
                                close(connfd);
                                close(listenfd);
                                string ip_peer = p.second.first;
                                int port_to_be_sent = p.first;
                                //CREATE TCP SOCKET
                                
                                listenfd = socket(AF_INET, SOCK_STREAM, 0);
                                bzero(&servaddr, sizeof(servaddr));
                                servaddr.sin_family = AF_INET;
                                servaddr.sin_addr.s_addr = inet_addr(ip_peer.c_str());
                                servaddr.sin_port = htons(PEER_PORT);
                                if(connect(listenfd, (struct sockaddr*)&servaddr, sizeof(servaddr))<0){
                                    exit(0);
                                }
                                // MESSAGE FORMAT: TASK FILE_NAME NUM_ASSIGNED NUM_PORTS IP_RECEIVER PORT_NUMBER
                                string message = "TASK "+filename+" "+to_string(p.second.second)+" "+to_string(numports)+" "+ip_rec+" "+to_string(port_to_be_sent);
                                char task_mes[1000];
                                bzero(task_mes, sizeof(task_mes));
                                for(int i=0;i<message.size();i++){
                                    task_mes[i] = message[i];
                                }
                                write(listenfd,task_mes,sizeof(task_mes));
                                //************************
                                exit(0);
                            }
                        }
                            
                        bzero(msg,sizeof(msg));
                        read(connfd,msg,sizeof(msg));
                        string msg_new(msg);
                        if(msg_new == "ACK"){
                            cout<<"DATA RECEIVED BY RECEIVER SUCCESSFULLY!\n";
                            close(connfd);
                            exit(0);
                        }
                        else{
                            cout<<"TRANSACTION FAILED AFTER 2 Attempts!\n";
                            close(connfd);
                            exit(0);
                        }

                    }
                    else{
                        vector<string> registration1;
                        map<string,vector<string>> fileSys1;
                        updateDataStructures(registration1,fileSys1);
                        vector<string> IPs = fileSys1[filename];
                        random_shuffle(IPs.begin(),IPs.end());
                        vector<pair<int, pair<string, int>>> nack_trans; // PORT to be sent, IP of peer , Offset
                        int count = 0;
                        for(int i=0;i<IPs.size();i++){
                            nack_trans.push_back({failed_ports[count].first, {IPs[i], failed_ports[count].second}});
                            count++;
                            if(count>=failed_ports.size()){
                                break;
                            }
                        }

                        string message_rdet = "RDET";
                        for(auto el:nack_trans){
                            message_rdet = message_rdet + " " + el.second.first;
                        }
                        char rdet_mes[1000];
                        bzero(rdet_mes, sizeof(rdet_mes));
                        for(int i=0;i<message_rdet.size();i++){
                            rdet_mes[i] = message_rdet[i];
                        }
                        cout<<message_rdet<<" is the sent message to receiver for nack!\n";
                        write(connfd, rdet_mes, sizeof(rdet_mes));

                        for(auto p:nack_trans){
                            if(fork()==0){
                                close(connfd);
                                close(listenfd);
                                string ip_peer = p.second.first;
                                int port_to_be_sent = p.first;
                                cout<<"Sent Port is: "<<port_to_be_sent<<"\n";
                                //CREATE TCP SOCKET
                                
                                listenfd = socket(AF_INET, SOCK_STREAM, 0);
                                bzero(&servaddr, sizeof(servaddr));
                                servaddr.sin_family = AF_INET;
                                servaddr.sin_addr.s_addr = inet_addr(ip_peer.c_str());
                                servaddr.sin_port = htons(PEER_PORT);
                                cout<<"Trying to connect to "<<ip_peer<<" at port "<<PEER_PORT<<"\n";
                                if(connect(listenfd, (struct sockaddr*)&servaddr, sizeof(servaddr))<0){
                                    exit(0);
                                }
                                // MESSAGE FORMAT: TASK FILE_NAME NUM_ASSIGNED NUM_PORTS IP_RECEIVER PORT_NUMBER
                                string message = "TASK "+filename+" "+to_string(p.second.second)+" "+to_string(numports)+" "+ip_rec+" "+to_string(port_to_be_sent);
                                char task_mes[1000];
                                bzero(task_mes, sizeof(task_mes));
                                for(int i=0;i<message.size();i++){
                                    task_mes[i] = message[i];
                                }
                                write(listenfd,task_mes,sizeof(task_mes));
                                //************************
                                exit(0);
                            }
                        }
                            
                        bzero(msg,sizeof(msg));
                        read(connfd,msg,sizeof(msg));
                        string msg_new(msg);
                        if(msg_new == "ACK"){
                            cout<<"DATA RECEIVED BY RECEIVER SUCCESSFULLY!\n";
                            close(connfd);
                            exit(0);
                        }
                        else{
                            cout<<"TRANSACTION FAILED AFTER 2 Attempts!\n";
                            close(connfd);
                            exit(0);
                        }
                    }
                }
            }
            close(connfd);
        }
    }
}