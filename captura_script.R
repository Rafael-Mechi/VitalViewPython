df_dados <- rbind(machine_data)

mediaRam <- mean(df_dados$memory_percent)
mediaCpu <- mean(df_dados$cpu_percent)


head(df_dados)
str(df_dados)
summary(df_dados)


names(df_dados)
head(df_dados$cpu_percent)

dim(df_dados)

str(df_dados)






#grafico cpu
hist(df_dados$cpu_percent[df_dados$cpu_percent <= 100], 
     main = "Histograma de uso de cpu", 
     xlab = "cpu",
     ylab = "Porcentagem",
     col = "skyblue",
     border = FALSE)

#grafico ram
hist(df_dados$memory_percent, 
     main = "Histograma de uso de RAM", 
     xlab = "ram",
     ylab = "Porcentagem",
     col = "red",
     border = FALSE)

####TESTE###

plot(df_dados$memory_percent,
     main = "Dispersão de uso de RAM",
     xlab = "Indice da captura",
     ylab = "Porcentagem",
     col = "black")

abline(h = mediaRam, col = "red", lwd = 2, lty = 2)
legend("topleft", legend = paste("Média =", round(mediaRam, 2)),
       col = "red", lwd = 2, lty = 2)


plot(df_dados$cpu_percent,
     main = "Dispersão de uso de CPU",
     xlab = "Indice da captura",
     ylab = "Porcentagem",
     col = "black")

abline(h = mediaCpu, col = "red", lwd = 2, lty = 2)
legend("topleft", legend = paste("Média =", round(mediaCpu, 2)),
       col = "red", lwd = 2, lty = 2)

