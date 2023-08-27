import './Header.css'
import logo from '../assets/logo.png'

function Header() {
  return (
    <>
      <div className='container' id='header'>
        <img src={logo} className='logo'/>
        <h2>
          <span style={{"color": "#E07A5F"}}>Database of </span> 
          <span style={{"color": "#3D405B"}}>Urban Transport Times</span>
        </h2>
      </div>
    </>
  )
}

export default Header
