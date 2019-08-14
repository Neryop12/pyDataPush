var vnombre=0;
	var vtelefono=0;
	var vemail=0;
	var vcedula=0;
	var vplan=0;
	var vtipo=0;
	var sender_email = $('#email').val();
	var check=0;
	var pais;
	var valn=1;
	var valt=1;
	var vale=1;
//Inicia Validación de Nombre Enviar
$('#nombre').keyup(function(){
	if($('#nombre').val().length==1){
        valn++;
        if(valn==5){
			window.dataLayer.push({'event':'campo_validado'});
			window.dataLayer.push({'campo_validado':'Campo Nombre'});
        }
	}
	else if($('#nombre').val().length>'5'){
		$('#nombre').removeClass('invalid').addClass('valid');
		$('#nombre-requerido').hide();
		window.dataLayer.push({'event':'campo_validado'});
		window.dataLayer.push({'campo_validado':'Campo Nombre'});
	}
	else if($('#nombre').val().length<'5'){
		$('#nombre').removeClass('valid').addClass('invalid');
		window.dataLayer.push({'event':'Error_campo_formulario'});
		window.dataLayer.push({'Error_campo':'Error Nombre'});
	}
});
//Inicia Validación de Nombre Enviar
/*
$('#cedula').keyup(function(){
	if($('#cedula').val().length==1){
        valn++;
        if(valn==8){
			window.dataLayer.push({'event':'campo_validado'});
			window.dataLayer.push({'campo_validado':'Campo Cédula'});
        }
	}
	else if($('#cedula').val().length=='9'){
		$('#cedula').removeClass('invalid').addClass('valid');
		$('#cedula-requerido').hide();
		window.dataLayer.push({'event':'campo_validado'});
		window.dataLayer.push({'campo_validado':'Campo Cédula'});
	}
	else if($('#cedula').val().length<'9'){
		$('#cedula').removeClass('valid').addClass('invalid');
		window.dataLayer.push({'event':'Error_campo_formulario'});
		window.dataLayer.push({'Error_campo':'Error Cédula'});
	}
});*/
//Inicia Validación de Telefono Enviar
$('#telefono').keyup(function(){
	if($('#telefono').val().length==2){
        valt++;
        if(valt==2){
			window.dataLayer.push({'event':'campo_validado'});
			window.dataLayer.push({'campo_validado':'Campo Teléfono'});
        }
	}
	else if($('#telefono').val().length=='8'){
		$('#telefono').removeClass('invalid').addClass('valid');
		$('#telefono-requerido').hide();
		window.dataLayer.push({'event':'campo_validado'});
		window.dataLayer.push({'campo_validado':'Campo Teléfono'});
	}
	else if($('#telefono').val().length<'8'){
		$('#telefono').removeClass('valid').addClass('invalid');
		window.dataLayer.push({'event':'Error_campo_formulario'});
		window.dataLayer.push({'Error_campo':'Error Teléfono'});
	}
});
//Inicia Validación de Email Enviar

$('#email').keyup(function(){
	
	if (validate_Email($('#email').val())) {
		$('#email').removeClass('invalid').addClass('valid');
		$('#email-requerido').hide();
		window.dataLayer.push({'event':'campo_validado'});
			window.dataLayer.push({'campo_validado':'Campo Email'});
	}
	else{
		$('#email').removeClass('valid').addClass('invalid');
		window.dataLayer.push({'event':'Error_campo_formulario'});
		window.dataLayer.push({'Error_campo':'Error Email'});
	}
});
//Inicia Validación de Tipo Enviar
$('input[name=tipo]').click(function(){
	$('#tipo-requerido').hide();
	window.dataLayer.push({'event':'campo_validado'});
	window.dataLayer.push({'campo_validado':'Campo Tipo Usuario'});
});
//Inicia Validación de Quiero Enviar

$('input[name=quiero]').change(function(){
	$('#quiero').removeClass('invalid').addClass('valid');
	$('#quiero-requerido').hide();
	window.dataLayer.push({'event':'campo_validado'});
	window.dataLayer.push({'campo_validado':'Plan'});
});
//VALIDACION AL DAR CLIC EN EL BOTON ENVIAR SI LOS CAMPOS ESTAN LLENOS
	$('.agregar').click(function(){
		if($('input[name=tipo]:checked').length<'1'){
			$('#tipo-requerido').show().addClass('animated fadeIn');
			window.dataLayer.push({'event':'Error_campo_formulario'});
			window.dataLayer.push({'Error_campo':'Error Tipo Usuario'});
		}
		else{
			$('#tipo-requerido').hide();
			vtipo=1;
			window.dataLayer.push({'event':'campo_validado'});
			window.dataLayer.push({'campo_validado':'Campo Tipo Usuario'});
		}
		if($('#quiero').val() == null){
			$('#quiero').addClass('invalid');
			$('#quiero-requerido').show().addClass('animated fadeIn');
			window.dataLayer.push({'event':'Error_campo_formulario'});
			window.dataLayer.push({'Error_campo':'Error Plan'});
		}
		else{
			$('#quiero').removeClass('invalid');
			$('#quiero-requerido').hide();
			vquiero=1;
			window.dataLayer.push({'event':'campo_validado'});
			window.dataLayer.push({'campo_validado':'Campo Plan'});
		}
		if($('#nombre').val().length<'5'){
			$('#nombre').addClass('invalid');
			$('#nombre-requerido').show().addClass('animated fadeIn');
			window.dataLayer.push({'event':'Error_campo_formulario'});
			window.dataLayer.push({'Error_campo':'Error Nombre'});
		}
		else{
			$('#nombre').removeClass('invalid');
			$('#nombre-requerido').hide();
			vnombre=1;
			window.dataLayer.push({'event':'campo_validado'});
			window.dataLayer.push({'campo_validado':'Campo Nombre'});
		}
		/*
		if($('#cedula').val().length<'9'){
			$('#cedula').addClass('invalid');
			$('#cedula-requerido').show().addClass('animated fadeIn');
			window.dataLayer.push({'event':'Error_campo_formulario'});
			window.dataLayer.push({'Error_campo':'Error Cédula'});
		}
		else{
			$('#cedula').removeClass('invalid');
			$('#cedula-requerido').hide();
			vcedula=1;
			window.dataLayer.push({'event':'campo_validado'});
			window.dataLayer.push({'campo_validado':'Campo Cédula'});
		}*/
		if($('#telefono').val().length<'8'){
			$('#telefono').addClass('invalid');
			$('#telefono-requerido').show().addClass('animated fadeIn');
			window.dataLayer.push({'event':'Error_campo_formulario'});
			window.dataLayer.push({'Error_campo':'Error Teléfono'});
		}
		else{
			$('#telfono').removeClass('invalid');
			$('#telefono-requerido').hide();
			vtelefono=1;
			window.dataLayer.push({'event':'campo_validado'});
			window.dataLayer.push({'campo_validado':'Campo Teléfono'});
		}
		/*if(validate_Email($('#email').val())){
			$('#email').removeClass('invalid');
			$('#email-requerido').hide();
			vemail=1;
			window.dataLayer.push({'event':'campo_validado'});
			window.dataLayer.push({'campo_validado':'Campo Email'});
		}
		else{
			$('#email').addClass('invalid');
			$('#email-requerido').show().addClass('animated fadeIn');
			window.dataLayer.push({'event':'Error_campo_formulario'});
			window.dataLayer.push({'Error_campo':'Error Email'});
		}*/
//INICIA LA VALIDACIÓN SI TODOS LOS CAMPOS ESTAN LLENOS ENVIA POR AJAX
	if(vnombre==1 && vtelefono==1&& vtipo==1 ){
	event.preventDefault();
	window.dataLayer.push({'event':'datos-validados'});
	var vUsuario=$('#quiero').val();
	var vQuiero=$('#quiero').val();
	//Creamos la Variable que recibira el "Value" de todos los Input que esten dentro del Form
	var obtener = $("#form_test").serialize();
	var data = {
		'action' : 'add',
		'pais': elpais,
		'source':source,
		'medium':medium,
		'campaign':campaign,
		'content':content
	  };
	data = obtener + '&' + $.param(data);
	$.ajax({
		url: "controllers/cliente_controller.php",
		data: data,
		type: "POST",
		success: function(data) {
		//ENVIO DE EVENTO Y VARIABLE A GTM
		window.dataLayer.push({'event':'Envioformulario'});
		//IMPRESIÓN DE ENVIO EXITOSO
			swal({
				title: "¡ Gracias por tu tiempo!",
				text: "En breve un asesor se contactará contigo.",
				type: "success",
				confirmButtonClass: "btn-danger",
				confirmButtonText: "Continuar",
				closeOnConfirm: false
			},
			function(){
				//REDIRIGE A PAIS
				window.location.href="https://www.claro.com.gt/personas/";
			  });
			 //Mensaje de Datos Correctamente Insertados
		},
		error: function( jqXhr, textStatus, errorThrown ){
			window.dataLayer.push({'event':'error_envio'});
            swal({
				title: "Ops!",
				text: "Tenemos un problema con el formulario!",
				type: "warning",
				confirmButtonClass: "btn-danger",
				confirmButtonText: "Continuar",
			  });
			  return false;
        }
	}); //Terminamos la Funcion Ajax
		}
		else{
			window.dataLayer.push({'event':'error_envio'});
		//ERROR AL ENVIAR EL FORMULARIO
			swal({
				title: "Ops!",
				text: "Tenemos un problema con el formulario!",
				type: "warning",
				confirmButtonClass: "btn-danger",
				confirmButtonText: "Continuar",
			  });
			  $('.cont-form').removeClass('fadeInUp');
			  $('.cont-form').removeClass('shake');
			setTimeout(function() {
				$('.cont-form').addClass('animated shake');
			  }, 100);
			}
		return false;
	});

	function validate_Email(sender_email) {
		var expression = /^[\w\-\.\+]+\@[a-zA-Z0-9\.\-]+\.[a-zA-z0-9]{2,4}$/;
		if (expression.test(sender_email)) {
		return true;
		}
		else {
		return false;
		}
		}

		$( document ).ready(function() {
			window.dataLayer.push({'pagina-inicio':'Inicio'});
			window.dataLayer.push({'event':'pagina-inicio'});
		});