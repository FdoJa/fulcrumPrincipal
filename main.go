package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"

	pb "github.com/FdoJa/ServidoresFulcrum/proto"
	"google.golang.org/grpc"
)

var timeVector [3]int
var listaLogs1 *pb.LogList
var listaLogs2 *pb.LogList

type fulcrumServer struct {
	pb.UnimplementedInformantesServer
}

func (s *fulcrumServer) AgregarBase(ctx context.Context, base *pb.Base) (*pb.Recepcion, error) {
	log.Printf("Comando recibido: AgregarBase\n")
	log.Printf("- Datos: %s %s %s\n", base.Sector, base.Base, base.Soldados)

	filePath := "/app/" + base.Sector + ".txt"

	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Fatalf("Error al leer archivo: %v", err)
	}
	defer file.Close()

	data := base.Sector + " " + base.Base + " " + base.Soldados + " \n"
	_, err = file.WriteString(data)
	if err != nil {
		log.Fatalf("Error al escribir en el archivo: %v", err)
	}

	// Luego de realizar la correcta modificación, pasar a colocarla en los logs
	filePathLog := "/app/log.txt"

	fileLog, err := os.OpenFile(filePathLog, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Fatalf("Error en manejo de logs: %v", err)
	}
	defer fileLog.Close()

	currentTime := time.Now()

	logLine := currentTime.Format("15:04:05.000000") + "AgregarBase " + base.Sector + " " + base.Base + " " + base.Soldados + " \n"
	_, err = fileLog.WriteString(logLine)
	if err != nil {
		log.Fatalf("Error al escribir en el archivo de logs: %v", err)
	}

	return &pb.Recepcion{
		Ok: "OK",
	}, nil
}

func (s *fulcrumServer) RenombrarBase(ctx context.Context, nuevaBase *pb.BaseModificada) (*pb.Recepcion, error) {
	log.Printf("Comando recibido: RenombrarBase\n")
	log.Printf("- Datos: %s %s %s\n", nuevaBase.Sector, nuevaBase.Base, nuevaBase.ActualizacionBase)

	filePath := "/app/" + nuevaBase.Sector + ".txt"

	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		log.Fatalf("Error al leer archivo: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var text []string

	for scanner.Scan() {
		line := scanner.Text()
		data := strings.Fields(line)

		base := data[1]
		if base == nuevaBase.Base {
			data[1] = nuevaBase.ActualizacionBase
		}

		newLine := strings.Join(data, " ")
		text = append(text, newLine)
	}

	file.Seek(0, 0)
	file.Truncate(0)

	for _, linea := range text {
		_, err := file.WriteString(linea + "\n")
		if err != nil {
			log.Fatalf("Error al modificar archivo: %v", err)
		}
	}

	// Luego de realizar la correcta modificación, pasar a colocarla en los logs
	filePathLog := "/app/log.txt"

	fileLog, err := os.OpenFile(filePathLog, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Fatalf("Error en manejo de logs: %v", err)
	}
	defer fileLog.Close()

	currentTime := time.Now()

	logLine := currentTime.Format("15:04:05.000000") + "RenombrarBase " + nuevaBase.Sector + " " + nuevaBase.Base + " " + nuevaBase.ActualizacionBase + " \n"
	_, err = fileLog.WriteString(logLine)
	if err != nil {
		log.Fatalf("Error al escribir en el archivo de logs: %v", err)
	}

	return &pb.Recepcion{
		Ok: "OK",
	}, nil
}

/*
func modificarBase(sector string, base string, nuevaBase string, merge bool) {
	filePath := "/app/" + sector + ".txt"

	currentTime := time.Now()
	fmt.Println("RenombrarBase: ", currentTime.Format("15:04:05.000000"))

	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		log.Fatalf("Error al leer archivo: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var text []string

	for scanner.Scan() {
		line := scanner.Text()
		data := strings.Fields(line)

		base := data[1]
		if base == base {
			data[1] = nuevaBase
		}

		newLine := strings.Join(data, " ")
		text = append(text, newLine)
	}

	file.Seek(0, 0)
	file.Truncate(0)

	for _, linea := range text {
		_, err := file.WriteString(linea + "\n")
		if err != nil {
			log.Fatalf("Error al modificar archivo: %v", err)
		}
	}

	if merge {
		return
	}

	// Luego de realizar la correcta modificación, pasar a colocarla en los logs
	filePathLog := "/app/log.txt"

	fileLog, err := os.OpenFile(filePathLog, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Fatalf("Error en manejo de logs: %v", err)
	}
	defer fileLog.Close()

	logLine := "RenombrarBase " + sector + " " + base + " " + nuevaBase + " \n"
	_, err = fileLog.WriteString(logLine)
	if err != nil {
		log.Fatalf("Error al escribir en el archivo de logs: %v", err)
	}
}
*/

func (s *fulcrumServer) ActualizarValor(ctx context.Context, actualizar *pb.ActualizarSoldados) (*pb.Recepcion, error) {
	log.Printf("Comando recibido: ActualizarValor\n")
	log.Printf("- Datos: %s %s %s\n", actualizar.Sector, actualizar.Base, actualizar.ActualizacionSoldados)

	actualizarCantidad(actualizar.Sector, actualizar.Base, actualizar.ActualizacionSoldados, false)

	return &pb.Recepcion{
		Ok: "OK",
	}, nil
}

func actualizarCantidad(sector string, base string, soldados string, merge bool) {
	filePath := "/app/" + sector + ".txt"

	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		// En caso de que no existe el archivo, se crea con un valor dummy para soldados = 0
		file, err = os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			log.Fatalf("Error al abrir o crear el archivo: %v", err)
		}

		data := sector + " " + base + " " + soldados + " \n"
		_, err = file.WriteString(data)
		if err != nil {
			log.Fatalf("Error al escribir en el archivo: %v", err)
		}

		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var text []string

	for scanner.Scan() {
		line := scanner.Text()
		data := strings.Fields(line)

		baseTxt := data[1]
		if baseTxt == base {
			data[2] = soldados
		}

		newLine := strings.Join(data, " ")
		text = append(text, newLine)
	}

	file.Seek(0, 0)
	file.Truncate(0)

	for _, linea := range text {
		_, err := file.WriteString(linea + "\n")
		if err != nil {
			log.Fatalf("Error al modificar archivo: %v", err)
		}
	}

	if merge {
		return
	}

	// Luego de realizar la correcta modificación, pasar a colocarla en los logs

	filePathLog := "/app/log.txt"

	fileLog, err := os.OpenFile(filePathLog, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Fatalf("Error en manejo de logs: %v", err)
	}
	defer fileLog.Close()

	currentTime := time.Now()

	logLine := currentTime.Format("15:04:05.000000") + "ActualizarValor " + sector + " " + base + " " + soldados + " \n"
	_, err = fileLog.WriteString(logLine)
	if err != nil {
		log.Fatalf("Error al escribir en el archivo de logs: %v", err)
	}
}

func (s *fulcrumServer) BorrarBase(ctx context.Context, borrar *pb.Base) (*pb.Recepcion, error) {
	log.Printf("Comando recibido: BorrarBase\n")
	log.Printf("- Datos: %s %s\n", borrar.Base, borrar.Sector)

	currentTime := time.Now()
	fmt.Println("RenombrarBase: ", currentTime.Format("15:04:05.000000"))

	borrarDato(borrar.Sector, borrar.Base, false)

	return &pb.Recepcion{
		Ok: "OK",
	}, nil
}

func borrarDato(sector string, base string, merge bool) {
	filePathData := "/app/" + sector + ".txt"

	fileData, err := os.OpenFile(filePathData, os.O_RDWR, 0644)
	if err != nil {
		if merge {
			return
		}

		log.Fatalf("Error al leer archivo: %v", err)
	}
	defer fileData.Close()

	scanner := bufio.NewScanner(fileData)
	var text []string

	for scanner.Scan() {
		line := scanner.Text()
		data := strings.Fields(line)

		baseTxt := data[1]
		if baseTxt == base {
			continue
		}

		newLine := strings.Join(data, " ")
		text = append(text, newLine)
	}

	fileData.Seek(0, 0)
	fileData.Truncate(0)

	for _, linea := range text {
		_, err := fileData.WriteString(linea + "\n")
		if err != nil {
			log.Fatalf("Error al modificar archivo: %v", err)
		}
	}

	if merge {
		return
	}

	// Luego de realizar la correcta modificación, pasar a colocarla en los logs
	filePathLog := "/app/log.txt"

	fileLog, err := os.OpenFile(filePathLog, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Fatalf("Error en manejo de logs: %v", err)
	}
	defer fileLog.Close()

	currentTime := time.Now()

	logLine := currentTime.Format("15:04:05.000000") + "BorrarBase " + sector + " " + base + " \n"
	_, err = fileLog.WriteString(logLine)
	if err != nil {
		log.Fatalf("Error al escribir en el archivo de logs: %v", err)
	}
}

func main() {
	lis, err := net.Listen("tcp", ":50052")
	if err != nil {
		log.Fatalf("Fallo en escuchar: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterInformantesServer(s, &fulcrumServer{})

	fmt.Println("Servidor Fulcrum escuchando")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Fallo en serve: %v", err)
	}

	//timer := time.NewTicker(1 * time.Minute)

	//fulcrum1Addr := ""
	//fulcrum2Addr := ""

	/*
		for {
			select {
			case <-timer.C:
				conn1, err := grpc.Dial(fulcrum1Addr, grpc.WithInsecure())
				if err != nil {
					log.Fatalf("No se pudo conectar al servidor Fulcrum 2: %v", err)
				}
				defer conn1.Close()

				client1 := pb.NewConsistenciaClient(conn1)

				res, err := client1.ConseguirLogs(context.Background(), &pb.Recepcion{
					Ok: "Ok",
				})

				if err != nil {
					log.Fatalf("Error al pedir Logs de servidor Fulcrum 2: %v", err)
				} else {
					listaLogs1 = &pb.LogList{
						ListaLogs: res.ListaLogs,
					}
				}

				//	Otro servidor
				conn2, err := grpc.Dial(fulcrum2Addr, grpc.WithInsecure())
				if err != nil {
					log.Fatalf("No se pudo conectar al servidor Fulcrum 3: %v", err)
				}
				defer conn2.Close()

				client2 := pb.NewConsistenciaClient(conn2)

				res2, err := client2.ConseguirLogs(context.Background(), &pb.Recepcion{
					Ok: "Ok",
				})

				if err != nil {
					log.Fatalf("Error al pedir Logs de servidor Fulcrum 3: %v", err)
				} else {
					listaLogs1 = &pb.LogList{
						ListaLogs: res2.ListaLogs,
					}
				}
			}
		}
	*/
}
