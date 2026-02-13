from fastapi import APIRouter, HTTPException, Depends
from firebase_admin import auth, firestore
from typing import List
from ..schemas import UsuarioResponse, UsuarioUpdate
from ..services.security import es_admin, es_usuario, get_current_user

router = APIRouter(
    prefix="/usuarios", 
    tags=["Gestión de Usuarios (CRUD)"]
)
DEPARTAMENTOS_VALIDOS = [
    "Contabilidad", "Cobranza", "Jurídico", 
    "Administración", "Sistemas", "Marketing", "Desarrollo"
]
@router.get("/", dependencies=[Depends(es_usuario)])
def listar_usuarios():
    """
    1.1 y 1.2 Devuelve los departamentos y dentro los usuarios correspondientes.
    """
    try:
        db = firestore.client()
        docs = db.collection('usuarios').stream()

        agrupados = {depto: [] for depto in DEPARTAMENTOS_VALIDOS}
        agrupados["Sin Asignar"] = [] 
        for doc in docs:
            data = doc.to_dict()
       
            user_info = {
                "id": doc.id,
                "nombre": data.get("nombre", "Sin Nombre"),
                "email": data.get("email", "Sin Email"),
                "rol": data.get("rol", "usuario"),
                "departamento": data.get("departamento", "Sin Asignar"),
                "creado_el": data.get("creado_el", None)
            }
            
          
            depto = data.get("departamento", "Sin Asignar")
            if depto in agrupados:
                agrupados[depto].append(user_info)
            else:
                agrupados["Sin Asignar"].append(user_info)
                
        return agrupados 
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error agrupando usuarios: {str(e)}")

@router.get("/{uid}", response_model=UsuarioResponse, dependencies=[Depends(es_usuario)])
async def obtener_usuario(uid: str):
    """Busca un perfil específico por su UID."""
    try:
        doc = firestore.client().collection("usuarios").document(uid).get()
        if not doc.exists:
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
        
        data = doc.to_dict()
        return {
            "id": doc.id,
            "nombre": data.get("nombre", "Sin Nombre"),
            "email": data.get("email", "Sin Email"),
            "rol": data.get("rol", "usuario"),
            "departamento": data.get("departamento", "Sin Asignar"),
            "creado_el": data.get("creado_el", None)
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
@router.put("/{uid}", dependencies=[Depends(es_admin)])
async def actualizar_usuario(uid: str, datos: UsuarioUpdate, solicitante: dict = Depends(get_current_user)):
    try:
        db = firestore.client()
        
      
        user_ref = db.collection("usuarios").document(uid)
        doc_objetivo = user_ref.get()
        
        if not doc_objetivo.exists:
            raise HTTPException(status_code=404, detail="El usuario no existe")
            
        data_actual = doc_objetivo.to_dict()
        rol_actual_objetivo = data_actual.get("rol") 
        rol_solicitante = solicitante.get("rol")     
        uid_solicitante = solicitante.get("uid")     

    
        if rol_actual_objetivo == "super_admin" and uid_solicitante != uid:
            raise HTTPException(
                status_code=403, 
                detail="Protección de Jerarquía: No puedes modificar los datos de un Super Admin."
            )

        if datos.rol == "super_admin" and rol_solicitante != "super_admin":
            raise HTTPException(
                status_code=403, 
                detail="Permiso denegado: Solo los Super Admins pueden asignar ese rango."
            )
            
        if rol_solicitante == "usuario" and uid_solicitante != uid:
            raise HTTPException(
                status_code=403, 
                detail="No tienes permisos para editar perfiles ajenos."
            )


        campos_enviados = datos.dict(exclude_unset=True)
        
        if not campos_enviados:
            return {"status": "info", "message": "No mandaste nada para actualizar."}

        auth_updates = {}
        if "email" in campos_enviados:
            auth_updates['email'] = datos.email
            
        if "password" in campos_enviados:
            if datos.password and len(datos.password) >= 6:
                auth_updates['password'] = datos.password
            elif datos.password:
                raise HTTPException(status_code=400, detail="Password muy corto (mínimo 6 caracteres)")

        if auth_updates:
            try:
                auth.update_user(uid, **auth_updates)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"Error en Firebase Auth: {str(e)}")

        firestore_updates = {k: v for k, v in campos_enviados.items() if k != 'password'}

        if firestore_updates:
           
            if "departamento" in firestore_updates:
                if firestore_updates["departamento"] not in DEPARTAMENTOS_VALIDOS:
                    raise HTTPException(
                        status_code=400, 
                        detail=f"Depto inválido. Opciones: {DEPARTAMENTOS_VALIDOS}"
                    )

            user_ref.update(firestore_updates)

        return {
            "status": "success", 
            "message": "Usuario actualizado correctamente respetando jerarquías",
            "campos_modificados": list(campos_enviados.keys())
        }

    except HTTPException as he: raise he
    except Exception as e: raise HTTPException(status_code=400, detail=str(e))

@router.delete("/{uid}", dependencies=[Depends(es_admin)])
async def eliminar_usuario(uid: str, solicitante: dict = Depends(get_current_user)): 
    """
    Elimina usuario respetando jerarquías: 
    Nadie borra a un Super Admin (solo él mismo).
    """
    try:
        db = firestore.client()
        
        user_ref = db.collection("usuarios").document(uid)
        doc_objetivo = user_ref.get()
        
        if doc_objetivo.exists:
            data_objetivo = doc_objetivo.to_dict()
            rol_objetivo = data_objetivo.get("rol")
            uid_solicitante = solicitante.get("uid")

            if rol_objetivo == "super_admin" and uid_solicitante != uid:
                raise HTTPException(
                    status_code=403, 
                    detail="Protección de Jerarquía: No puedes eliminar a un Super Admin."
                )
        try:
            auth.delete_user(uid)
            print(f"✓ Usuario {uid} eliminado de Authentication")
        except auth.UserNotFoundError:
            print(f"! Usuario {uid} no existía en Auth, procediendo con Firestore...")
        except Exception as auth_e:
            raise HTTPException(status_code=400, detail=f"Error en Auth: {str(auth_e)}")

        if doc_objetivo.exists:
            user_ref.delete()
            return {"status": "success", "message": f"Usuario {uid} eliminado correctamente."}
        else:
            return {"status": "info", "message": "El documento no existía en Firestore"}

    except HTTPException as he: raise he
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error general: {str(e)}")