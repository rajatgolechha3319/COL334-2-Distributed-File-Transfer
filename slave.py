import socket
import threading
import time

host = '10.184.7.6'
port = 13355
started = False
finalle_started  = False
line_num=1000
data_dict = {}
finalle_started_lock = threading.Lock()
started2=False
linerecv=[]
linenotrecv=[]

def recv_input(sock):
    input_buffer = b''
    while True:
        try:
            packet = sock.recv(1024)
        except:
            return None
        if not packet:
            return None
        input_buffer += packet
        if packet.endswith(b'\n'):
            break
    if not input_buffer.endswith(b'\n'):
        input_buffer += b'\n'
    return input_buffer.decode('utf-8')

def recieve_thread(server_socket):
    global started
    global finalle_started
    global started2
    global line_num
    global linenotrecv
    global linerecv
    qw=False
    while True:
        receive = server_socket.recv(2048).decode('utf-8')
        if receive=='start':
            started=True
            break
        
        time.sleep(1e-6)
    while True:
        try:
            if finalle_started==False and len(data_dict)<line_num:
                data = recv_input(server_socket)
                if not data:
                    break
                if data == 'done\n':
                    finalle_started=True
                    print("Asking the server")
                    # time.sleep(1)
                    # server_socket.sendall('send\n'.encode())
                else:
                    try:
                        lines = data.split("\n")
                        line_number = int(lines[0])
                        line_content = lines[1]
                    except Exception as e:
                        continue
                        #print(e)
                    if line_number not in data_dict:
                        data_dict[line_number] = line_content
                        linerecv.append(line_number)
                    print("Recieved from server ")
            else:
                if (qw==False):
                    linerecv.sort()
                qw=True
                linenotrecv=list(set(range(0,line_num))-set(linerecv))
                if(len(linenotrecv)==0):
                    print("All the data is recieved")
                    # server_socket.sendall('stop\n'.encode())
                    # print('all the data is received23')
                    # started2=True
                    # time.sleep(100)
                    
                # while(len(data_dict)<line_num):
                else:
                    snd=str(linenotrecv[0])+'\n'
                    print(snd)
                    server_socket.sendall(snd.encode())
                    print("Recieve sended")
                    data = recv_input(server_socket)
                    if not data:
                        break
                    print(data)
                
                # if not data:
                    # break
                    try:
                        lines = data.split("\n")
                        line_number = int(lines[0])
                        # print(line_number)
                        line_content = lines[1]

                    except Exception as e:
                        # print(data)
                        print(e)
                    print("Recieved from server ")
                    if line_number not in data_dict:
                        data_dict[line_number] = line_content
                        linerecv.append(line_number)
                    
                if len(data_dict)>=line_num:
                    server_socket.sendall('stop\n'.encode())
                    
                    # print('all the data is received for god\'s sake')
                    started2=True
                    # time.sleep(100)
                    # server_socket.close()
                    break
        except Exception as e:
            print(f'exception occured in recieve_thread : {e}')
            break

def vayu_thread_broadcast(vayu_socket,server_socket):
    global started
    global finalle_started
    global started2
    global line_num
    global linenotrecv
    server_address = ("10.17.51.115", 9801)
    sendline_command = b"SENDLINE\n"

    while True:
        if started==True:
            break
        time.sleep(1e-6)
    vayu_socket.settimeout(5)
    vayu_socket.connect(server_address)
    start = time.time()
    while True:
        try:
            print(len(data_dict))
            if finalle_started==False and len(data_dict)<line_num:
                # print(1)

                # print(2)
                vayu_socket.sendall(sendline_command)
                received_data = recv_input(vayu_socket)
                # print(3)
                try:
                    lines = received_data.split("\n")
                    line_number = int(lines[0])
                    line_cont = lines[1]
                except Exception as e:
                    continue
                if line_number == -1:
                    time.sleep(1e-6)
                    continue
                # print(4)
                if line_number not in data_dict:
                    data_dict[line_number]=line_cont
                    linerecv.append(line_number)
                try:
                    server_socket.sendall(received_data.encode())
                except Exception as e:
                    continue
                # print(5)
            else:
                if started2==True or len(data_dict)>=line_num:
                    file_name = "output.txt"
                    print(f"Data written to {file_name}")
                    print("Submitting...")
                    submit_command = b"SUBMIT\naseth@col334-672\n" + str(line_num).encode() + b"\n"
                    vayu_socket.sendall(submit_command)
                    for key in data_dict.keys():
                        vayu_socket.sendall(str(key).encode() + b"\n" + data_dict[key].encode() + b"\n")
                    finish = time.time()
                    print(f"Time taken: {finish - start}")
                    submission_success = recv_input(vayu_socket)
                    print(submission_success)
                    with open(file_name, "w") as file:
                        for key, value in data_dict.items():
                            file.write(f"Key: {key}, Value: {value}\n")
                    # time.sleep(100)
                    break
        except Exception as e:
            print('Error sending:', str(e))
            break
        
def main():
    global host
    global port
    host = input('Enter host: ')
    port = int(input('Enter port: '))
    ClientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    VayuSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print('Waiting for connection')
    try:
        ClientSocket.connect((host, port))
        print('Doofenshmirtz Evil Incorporated')
    except Exception as e:
        print(str(e))
        exit()
    receive_thread = threading.Thread(target=recieve_thread, args=(ClientSocket,))
    vayu_thread = threading.Thread(target=vayu_thread_broadcast, args=(VayuSocket,ClientSocket))
    receive_thread.start()
    vayu_thread.start()
    vayu_thread.join()
    receive_thread.join()
    print('Client threads closed.')
    ClientSocket.close()
    VayuSocket.close()
if __name__ == '__main__':
    main()