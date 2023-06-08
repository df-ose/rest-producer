package com.usuarios.data;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class Datos {

    private Integer eventId;
    private Integer id;
    private String  name;
    private String  lastname;
    private String  function;
    private String  email;
    private String  city;
    
	public String getcity() {
		
		return city;
	}
	
	public Integer geteventId() {
		
		return eventId;
	}

}