#include <bits/stdc++.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <unistd.h>
#include <filesystem>

#define SERVER_IP "192.168.130.250"
#define SERVER_REG_PORT 6969
#define PEER_PORT 9000
#define FIRST_PORT 9001
#define FUP_PORT 9002
#define FILE_DIRECTORY_PATH "/home/rj/Desktop/FileSystem"
#define ll long long

#include <stdio.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <arpa/inet.h> 
#include <strings.h> 
#include <sys/wait.h> 
#include<bits/stdc++.h>
#include <iostream>  
#include <fstream>
#include <signal.h>


#define server_addr "192.168.130.250"
#define server_port 8080
int concur_limit=4;


using namespace std;
namespace fs = std::filesystem;

vector<int> available_ports;


// receiver functions*******************************************************************************************

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


void handle_alarm(int signum) {
    // This function will be called when the timer expires.
    // You can add your termination logic here.
    // For example, you can use exit() to terminate the process.
    printf("Time's up! Terminating...\n");
    exit(0);
}

void fill_ports(){
    for(int i=0;i<concur_limit;i++){
        available_ports.push_back(6000+i);
    }
}


void receive_from_peers(int concur, vector<int> available_ports, vector<string> server_ip_input){


    string temp="";
    for(int i=0;i<concur;i++){
        int listenfd_1, clilen_1, connfd_1, childpid_1, childpid_2, n1_1;
        int status=0;
        struct sockaddr_in servaddr_1, cliaddr_1;
        char msg1_1[512];

        listenfd_1 = socket(AF_INET, SOCK_STREAM, 0);
        if (listenfd_1 == -1) { 
            cout<<("receiver socket- "+to_string(available_ports[i])+" creation failed...\n"); 
            exit(0); 
        } 
        else{
            // cout<<("receiver socket- "+to_string(available_ports[i])+" successfully created..\n");
        }
            

        if(setsockopt(listenfd_1, SOL_SOCKET, SO_REUSEADDR, &temp, sizeof(int))==-1){
            perror("setsockopt");
        }



        bzero(&servaddr_1, sizeof(servaddr_1));
        servaddr_1.sin_family = AF_INET;
        // servaddr_1.sin_addr.s_addr = inet_addr("127.0.0.2");
        servaddr_1.sin_addr.s_addr = htonl(INADDR_ANY);
        servaddr_1.sin_port = htons(available_ports[i]);

        if(bind(listenfd_1, (struct sockaddr *)&servaddr_1, sizeof(servaddr_1))!=0){
            cout<<("receiver socket- "+to_string(available_ports[i])+" bind failed...\n");  
            exit(0);
        }
        else{
            // cout<<("receiver socket- "+to_string(available_ports[i])+" successfully binded..\n");
        }
        


        // ************************************************************************
        // forked processes accepting files from peers

        if((childpid_1 = fork()) == 0){
            signal(SIGALRM, handle_alarm); // setting alarm
            alarm(7);

            // emptying the files

            string empty_s="";
            ofstream filestream_1(to_string(available_ports[i])+".txt");  
            if (filestream_1.is_open()){  
                filestream_1 <<empty_s;  
                filestream_1.close();  
            }  
            else cout<<"File opening is failed.";



            while(1){
                if(listen(listenfd_1, 1)!=0){
                    cout<<("receiver socket- "+to_string(available_ports[i])+" listening failed...\n"); 
                    exit(0);
                }
                else{
                    cout<<("Receiver: receiver socket- "+to_string(available_ports[i])+" listening..\n");
                }
                sleep(2);

                bzero(msg1_1, sizeof(msg1_1));
                bzero(&cliaddr_1, sizeof(cliaddr_1));
                clilen_1 = sizeof(cliaddr_1);
                
                connfd_1 = accept(listenfd_1, (struct sockaddr *)&cliaddr_1, (unsigned int*)&clilen_1);
                if(connfd_1<0){
                    cout<<("receiver socket- "+to_string(available_ports[i])+" connection accept failed...\n");
                }
                else{
                    // cout<<("receiver socket- "+to_string(available_ports[i])+" request received...\n");
                }
                
                // cout<<"lilili\n";

                ll ip_numeric=cliaddr_1.sin_addr.s_addr;
                string client_ip=ipconvert(ip_numeric);
                // cout<<client_ip<<" hoja\n";e


                // cout<<"l11\n";
                // cout<<"ll2 "<<available_ports[i]<<"\n";
                // cout<<"client ip: "<<client_ip<<" desired ip: \n";
                if(client_ip!=server_ip_input[i]){
                    cout<<"Receiver: connection rejected!\n";
                    close(connfd_1);
                }
                else{
                   break;
                } 
            }
            cout<<"Receiver: correct peer accepted on port "<<available_ports[i]<<"\n";
            close(listenfd_1);
            

            n1_1 = read(connfd_1, msg1_1, 512);
            string peer_message(msg1_1);

            string actual_message=peer_message.substr(5);

            // writing to file
            string filename=to_string(available_ports[i])+".txt";
            ofstream filestream(filename);  
            if (filestream.is_open()){        
                filestream <<actual_message;  
                filestream.close();  
            }  
            else cout<<"File opening is failed."; 





            close(connfd_1);
            close(listenfd_1);
            // cout<<"child process with socket "<<available_ports[i]<<" exiting...\n";
            exit(0);
        }
        close(listenfd_1);
        // cout<<"*****\n";
    }
    while(wait(NULL)>0);
}


// ****************************************************************************************



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

vector<string> getFiles(){
    vector<string> vec;
    string path = FILE_DIRECTORY_PATH;
    for(const auto & entry : fs::directory_iterator(path)){
        string file = entry.path().string();
        file = file.substr(path.size()+1);
        vec.push_back(file);
    }
    return vec;
}

string retrieveData(string filename, int num, int total){
    //num/total - (num+1)/total
    
    // return ("DATA"+to_string(num));

    //jhdgfdjfhkfrj
    //CHECK LENGTH
    //CHECK ceil(LENGTH/TOTAL)
    // num*ceil(LEN/TOTAL) -> (num+1)*ceil(LEN/TOTAL)
    string filepath = FILE_DIRECTORY_PATH;
    filepath = filepath + "/" + filename; 
    fstream file(filepath, ios::in);
    string data;
    getline(file,data);
    double size = data.size();
    size = size/total;
    int offset = ceil(size);
    if(num*offset >= data.size()){
        return "";
    }
    if((num+1)*offset < data.size()){
        return data.substr(num*offset, offset);
    }
    else{
        return data.substr(num*offset);
    }
}


int main(){
    if(fork()!=0){
        //PEER
        int sockfd;
        struct sockaddr_in servaddr, cliaddr;
        sockfd = socket(AF_INET,SOCK_STREAM,0);
        bzero(&servaddr, sizeof(servaddr));
        servaddr.sin_family = AF_INET;
        servaddr.sin_port = htons(SERVER_REG_PORT);
        servaddr.sin_addr.s_addr = inet_addr(SERVER_IP);
        connect(sockfd, (struct sockaddr*)&servaddr, sizeof(servaddr));
        char msg[1000];
        bzero(msg,sizeof(msg));
        string regmessage = "REG";
        vector<string> regfiles = getFiles();
        for(int i=0;i<regfiles.size();i++){
            regmessage = regmessage +" "+ regfiles[i];
        }
        for(int i=0;i<regmessage.size();i++){
            msg[i] = regmessage[i];
        }
        write(sockfd,msg,sizeof(msg));
        bzero(msg,sizeof(msg));
        read(sockfd,msg,sizeof(msg));
        string recack(msg);
        if(recack.substr(0,3)=="ACK"){
            cout<<"Registration Succesful!\n";
        }
        else{
            cout<<"Could not register!\n";
        }
        string my_ip = recack.substr(4);
        close(sockfd);

        if(fork()==0){
            for(;;){
                sockfd = socket(AF_INET,SOCK_STREAM,0);
                bzero(&servaddr, sizeof(servaddr));
                servaddr.sin_family = AF_INET;
                servaddr.sin_port = htons(SERVER_REG_PORT);
                servaddr.sin_addr.s_addr = inet_addr(SERVER_IP);
                connect(sockfd, (struct sockaddr*)&servaddr, sizeof(servaddr));
                char msg[1000];
                bzero(msg,sizeof(msg));
                string fupmessage = "FUP";
                vector<string> fupfiles = getFiles();
                for(int i=0;i<fupfiles.size();i++){
                    fupmessage = fupmessage +" "+ fupfiles[i];
                }
                for(int i=0;i<fupmessage.size();i++){
                    msg[i] = fupmessage[i];
                }
                write(sockfd,msg,sizeof(msg));
                sleep(2);
            }
        }
        int listenfd;
        listenfd = socket(AF_INET, SOCK_STREAM, 0);
        bzero(&servaddr, sizeof(servaddr));
        servaddr.sin_family = AF_INET;
        servaddr.sin_addr.s_addr = inet_addr(my_ip.c_str());
        servaddr.sin_port = htons(PEER_PORT);
        bind(listenfd, (struct sockaddr *)&servaddr, sizeof(servaddr));
        ll ip_peer_int = servaddr.sin_addr.s_addr;
        string ip_peer = ipconvert(ip_peer_int);
        
        listen(listenfd,5);
        int clilen,connfd;
        for(;;){
            clilen = sizeof(cliaddr);
            cout<<"WAITING FOR REQUEST!\n";
            connfd = accept(listenfd, (struct sockaddr *)&cliaddr, (socklen_t*)&clilen);
            if(fork()==0){
                cout<<"ACCEPTED TASK FROM SERVER!\n";
                close(listenfd);
                sleep(2);
                char taskbuf[1000];
                bzero(taskbuf,sizeof(taskbuf));
                read(connfd,taskbuf,sizeof(taskbuf));
                string taskmes(taskbuf);
                vector<string> task = split(taskmes);
                if(task[0]!="TASK"){
                    cout<<"ERROR IN TASK MESSAGE!\n";
                }
                // MESSAGE FORMAT: TASK FILE_NAME NUM_ASSIGNED NUM_PORTS IP_RECEIVER PORT_NUMBER
                int num_assigned = stoi(task[2]);
                int num_total = stoi(task[3]);
                int rec_port = stoi(task[5]);
                string rec_ip = task[4];
                string filename = task[1];

                // MESSAGE FORMAT: SEND NUM_ASSIGNED DATA

                string message = "SEND "+task[2]+" ";
                message = message + retrieveData(filename,num_assigned,num_total);
                bzero(taskbuf,sizeof(taskbuf));
                for(int i=0;i<message.size();i++){
                    taskbuf[i] = message[i];
                }
                int sockfd1 = socket(AF_INET,SOCK_STREAM,0);
                bzero(&servaddr, sizeof(servaddr));
                servaddr.sin_family = AF_INET;
                servaddr.sin_port = htons(rec_port);
                servaddr.sin_addr.s_addr = inet_addr(rec_ip.c_str());
                sleep(2);
                if(connect(sockfd1, (struct sockaddr*)&servaddr, sizeof(servaddr))<0){
                cout<<"CANNOT CONNECT at port "<<rec_port<<" and IP: "<<rec_ip<<"!\n";
                }
                else{
                    write(sockfd,taskbuf,sizeof(taskbuf));
                }
                
                exit(0);
            }   
            close(connfd); 
        }
    }
    else{
        //RECEIVER
        
        while(1){
            fill_ports();
            int sockfd, n;
            struct sockaddr_in servaddr;
            char sendline[512];
            char recvline[512];
            sockfd = socket(AF_INET, SOCK_STREAM, 0);
            if (sockfd == -1) {
                printf("socket creation failed...\n");
                cout<<"Receiver: error! exiting...\n";
                exit(0);
            }
            else{
                // printf("Socket successfully created..\n");
            }

            bzero(&servaddr, sizeof(servaddr));
            servaddr.sin_family = AF_INET;
            servaddr.sin_port = htons(server_port);
            servaddr.sin_addr.s_addr = inet_addr(server_addr);

            fgets(sendline, 512, stdin);
            n = strlen(sendline);

            string req_file_name(sendline);
            req_file_name=req_file_name.substr(4);
            req_file_name.pop_back();

            if(connect(sockfd, (struct sockaddr *)&servaddr, sizeof(servaddr)) !=0){
                printf("connection with the server failed...\n");
                continue;
            }
            else{
                cout<<"receiver: connected to server...\n";
            }
            
            write(sockfd, sendline, n);
            n = read(sockfd, recvline, 512);
            if (n < 0)
                printf("error reading\n");
            recvline[n] = 0;
            // fputs(recvline, stdout);
            // exit(0);



            /* ************************************************************************************************** */
            // receiving ip list from the server

            string ips(recvline);
            // ips.pop_back();
            
            // cout<<ips.length()<<" "<<ips<<'\n';

            vector<string> server_ip_input;
            string temp="";
            for(int i=0;i<ips.length();i++){
                if(ips[i]!=' '){
                    temp+=ips[i];
                }
                else{
                    server_ip_input.push_back(temp);
                    temp="";
                }
            }
            if(server_ip_input[0]!="DET"){
                cout<<"error: incorrect message  type received..\n";
                continue;
            }

            server_ip_input.erase(server_ip_input.begin());  // ip list processing complete
            if(temp=="NA"){
                cout<<"receiver: requested file doesnt exist!\n";
                continue;
            }
            int comm_port=stoi(temp);
            // cout<<comm_port<<" jj\n";
            temp="";
            int concur=min((int)server_ip_input.size(), concur_limit);

            // **********************************************************************************************************
            // establishing connection with a new port


            sockfd = socket(AF_INET, SOCK_STREAM, 0);
            if (sockfd == -1) {
                printf("new socket creation failed...\n");
                continue;
            }
            else{
                // printf("new Socket successfully created..\n");
            }


            bzero(&servaddr, sizeof(servaddr));
            servaddr.sin_family = AF_INET;
            servaddr.sin_port = htons((int)comm_port);
            servaddr.sin_addr.s_addr = inet_addr(server_addr);
            if(connect(sockfd, (struct sockaddr *)&servaddr, sizeof(servaddr))!=0){
                cout<<"reconnection with server failed...\n";
                continue;
            }
            else{
                // cout<<"connection with new port established...\n";
            }
            

            

            // *******************************************************************************************************
            // sending the opened ports back to server

            bzero(&sendline, sizeof(sendline));
            string com_message1="CON "+to_string(concur);
            for(int i=0;i<concur;i++){
                com_message1=com_message1+" "+to_string(available_ports[i])+" "+server_ip_input[i];
            }


            for(int i=0;i<com_message1.length();i++){
                sendline[i]=com_message1[i];
            }
            n = strlen(sendline);
            write(sockfd, sendline, n);
            // cout<<"PORT NUMBERS SENT...\n";




            /* ****************************************************************************************************** */
            // opening ports using fork

            
            receive_from_peers(concur, available_ports, server_ip_input);
            // cout<<"bahar\n";
            // while(wait(NULL)>0);
            // sleep(7);
            // cout<<"receiver: done...\n";


            // ***********************************************************************************************
            // checking the status of the received files

            bool failed_again=0;

            int counter=0;
            while(1){
                counter++;
                string not_received_ports="";
                vector<int> not_received;
                for(int i=0;i<concur;i++){
                    string srg_2="";  
                    ifstream filestream_2(to_string(available_ports[i])+".txt"); 
                    if (filestream_2.is_open())  
                    {  
                        
                        getline (filestream_2,srg_2);
                        // cout<<srg_2<<"hi\n";
                        filestream_2.close();
                        if(srg_2==""){
                            not_received.push_back(available_ports[i]);
                            not_received_ports+=(" "+to_string(available_ports[i]));
                        }  
                    }  
                    else 
                    {  
                        cout << "File opening is fail."<<endl;   
                    }
                }
                // cout<<not_received_ports<<"\n";
                if(not_received_ports==""){
                    // cout<<"normal execution\n";
                    break;
                } 
                

                // ********************************************************************************************************
                // re receiving files

                bzero(&sendline, sizeof(sendline));
                string com_message3="NAK"+not_received_ports;
                for(int i=0;i<com_message3.length();i++){
                    sendline[i]=com_message3[i];
                }
                n = strlen(sendline);
                write(sockfd, sendline, n);
                // cout<<com_message3<<" sent\n";

                if(counter==2){
                    cout<<"Receiver: File not received. Request again!\n";
                    failed_again=1;
                    break;
                }

                bzero(&recvline, sizeof(recvline));
                // cout<<"pahuncha\n";
                n = read(sockfd, recvline, 512);
                // cout<<"pahuncha 2\n";
                if (n < 0)
                    printf("error reading\n");
                recvline[n] = 0;
                string new_ips(recvline);
                // cout<<new_ips<<" received\n";

                // **********************
                vector<string> new_ip_input;
                string temp4="";
                for(int i=0;i<new_ips.length();i++){
                    if(new_ips[i]!=' '){
                        temp4+=new_ips[i];
                    }
                    else{
                        new_ip_input.push_back(temp4);
                        temp4="";
                    }
                }
                if(temp4!=""){
                    new_ip_input.push_back(temp4);
                }
                if(new_ip_input[0]!="RDET"){
                    cout<<"Receiver: error! incorrect message  type received..\n";
                    failed_again=1;
                    break;
                }

                new_ip_input.erase(new_ip_input.begin());  // ip list processing complete

                cout<<"Receiver: trying again to receive files....\n";
                // for(int i=0;i<new_ip_input.size();i++){
                //     cout<<new_ip_input[i]<<" ";
                // }
                // cout<<"gg\n";
                receive_from_peers(not_received.size(), not_received, new_ip_input);
                // while(wait(NULL)>0);
            }
            if(failed_again) continue;





            // ********************************************************************************************* 
            // telling server about the status

            bzero(&sendline, sizeof(sendline));
            string com_message2="ACK";
            for(int i=0;i<com_message2.length();i++){
                sendline[i]=com_message2[i];
            }
            n = strlen(sendline);
            write(sockfd, sendline, n);
            close(sockfd);


            // *****************************************************************************************************
            // received file assembly

            vector<string> final_file_parts(concur);
            for(int i=0;i<concur;i++){
                string srg;  
                ifstream filestream(to_string(available_ports[i])+".txt");  
                if (filestream.is_open())  
                {  
                    while ( getline (filestream,srg) )  
                    {  

                        string temp3="";
                        int offset=-1;
                        for(int j=0;j<srg.length();j++){
                            if(srg[j]!=' '){
                                temp3+=srg[j];
                            }
                            else{
                                offset=j+1;
                                break;
                            }
                        }
                        final_file_parts[stoi(temp3)]=srg.substr(offset);
                    }  
                    filestream.close();  
                }  
                else 
                {  
                    cout << "File opening is fail."<<endl;   
                }
            }

            // ************************************************************************************************************
            // final file output


            string final_file="";
            for(int i=0;i<final_file_parts.size();i++){
                final_file+=final_file_parts[i];
            }
            
            string file_path=FILE_DIRECTORY_PATH;
            file_path+=("/"+req_file_name);
            ofstream filestream(file_path);  
            if (filestream.is_open())
            {  
                filestream <<final_file;  
                filestream.close();  
            }  
            else{
                cout<<"File opening is fail.";
            } 



            cout<<"Receiver: FILE RECEIVED SUCCESSFULLY!\n";
        }


    }
}