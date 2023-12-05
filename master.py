import socket
import threading
import time
import sys 
import os
# is it good idea to use same lock everywhere
server_list = []
thread_list = []
line_num=1000
max_connection=1 # took care no. of connections
found_server = True
done_count=0
done_count_lock=threading.Lock()
data_dict = {}
broadcast_list_lock=threading.Lock()
broadcast_list_lock1=threading.Lock()
broadcast_list_lock2=threading.Lock()
broadcasting_started=False
sending_started = 0
found=False
lock=threading.Lock()

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

def starting_client():
    global broadcaasting_started
    # global found
    while True:
        command = input("Enter 'start' to initiate interaction: \n")
        if command.strip().lower()=='start':
            print("Starting the interaction with client and vayu")
            broadcaasting_started=True
            with broadcast_list_lock:
                for client_conn in server_list:
                    client_conn.sendall("start".encode())
            break
    print('all done starting_client disconnected')
    

            

def handling_client(conn,address):
    global done_count
    global max_connection
    global line_num
    global sending_started
    global found
    while True:
        if len(data_dict)<line_num:
            data = recv_input(conn)
            if not data:
                break
            if data=='stop\n':
                with done_count_lock:
                    done_count+=1
                    break
            try:
                lines = data.split("\n")
                line_numb = int(lines[0])
                line_cont = lines[1]
            except Exception as e:
                print(e)
            if line_numb not in data_dict:
                data_dict[line_numb] = line_cont
                with broadcast_list_lock1:
                    for client_conn in server_list:
                        if client_conn != conn:
                            client_conn.sendall(data.encode())
                if len(data_dict)>=line_num:
                    found = True
                print("Received from connected user {}: {}".format(address, line_numb))

        else:
            data = recv_input(conn)
            if not data:
                break
            if data!='stop\n':
                print(data, "from client")
                # print('recieved send command')
                print('sending data')
                lines = data.split("\n")
                try:
                    lin_num = int(lines[0])
                    response = str(lin_num) + '\n' + data_dict[lin_num] + '\n'
                    conn.sendall(response.encode())
                except:
                    conn.sendall('done\n'.encode())
            elif data=='stop\n':
                with done_count_lock:
                    done_count+=1
                    break
    with broadcast_list_lock:
        server_list.remove(conn)
        print("Client disconnected:", address)
    conn.close()
    print('all done handling_client_close disconnected')
    
            
    
def connecting_thread(server_socket):
    global max_connection
    global done_count
    count=0
    while done_count < max_connection:
        if done_count==max_connection:
            break
        conn, address = server_socket.accept()
        server_list.append(conn)
        print(f'client connected : {address}')
        client_thread = threading.Thread(target=handling_client, args=(conn, address))
        client_thread.start()
        thread_list.append(client_thread)
        count+=1
        if count>max_connection:
            conn.sendall("start".encode())
        print(done_count, max_connection)
        # if len(server_list)==max_connection:
        #     print("All clients connected")
    print('all done server disconnected')
    server_socket.close()
def broadcasting_thread():
    server_address = ("10.17.6.5", 9801)
    sendline_command = b"SENDLINE\n"
    global line_num
    global sending_started
    global done_count
    global found_server
    global max_connection
    global found
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
    s.settimeout(5)
    s.connect(server_address)
    start = time.time()
    while True:
        try:
            while len(data_dict) < line_num:
                print(len(data_dict))
                s.sendall(sendline_command)
                received_data = recv_input(s)
                try:
                    lines = received_data.split("\n")
                    line_number = int(lines[0])
                    line_content = lines[1]
                except Exception as e:
                    print(e)
                if line_number == -1:
                    time.sleep(1e-6)
                    continue
                if line_number not in data_dict:
                    data_dict[line_number] = line_content
                    with broadcast_list_lock2:
                        for ke in server_list:
                            try:
                                ke.sendall(received_data.encode())
                            except Exception as e:
                                continue
                if len(data_dict)>=line_num:
                    found=True
            
            if found and found_server:
                with broadcast_list_lock:
                        for client_conn in server_list:
                            client_conn.sendall('done\n'.encode())
                        found = False
                submit_command = b"SUBMIT\naseth@col334-672\n" + str(line_num).encode() + b"\n"
                s.sendall(submit_command)
                for key in range(line_num):
                    with broadcast_list_lock: # is it good idea to use same lock 
                        s.sendall(str(key).encode() + b"\n" + data_dict[key].encode() + b"\n")
                submission_success = recv_input(s)
                print(submission_success)
                finish = time.time()
                print(f"Time taken: {finish - start}")
                found_server = False
            if done_count==max_connection:    
                break
        except Exception as e:
            print("An error occurred in broadcasting_thread:", e)
            break
    print('all done broadcasting_thread_close disconnected')
    
    s.close()
    # exit(0)

def main():
    host = '10.184.7.6'
    port = int(input("Enter port number: "))
    global max_connection
    max_connection = int(input("Enter max number of connections: "))
    server_socket = socket.socket()
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((host, port))
    server_socket.listen(10)
    print("Server listening on {}:{}".format(host, port))
    connection_accept_thread = threading.Thread(target=connecting_thread, args=(server_socket,))
    connection_accept_thread.start()
    start_command_thread = threading.Thread(target=starting_client)
    start_command_thread.start()
    start_command_thread.join()
    broadcast_thread = threading.Thread(target=broadcasting_thread)
    broadcast_thread.start()
    broadcast_thread.join()
    os._exit(1)
    
if __name__ == "__main__":
    main()