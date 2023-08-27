import './Footer.css'
import logo from '../assets/logo.png'

function Footer() {

  return (
    <>
      <div id='footer'>
        <div className='container footerContainer'>
          <img src={logo} className='logo'/>
          <h2>
            <span style={{"color": "#E07A5F"}}>Database of </span>
            <span style={{"color": "#F2CC8F"}}>Urban Transport Times</span>
          </h2>
        </div>
      </div>
    </>
  )
}

export default Footer
